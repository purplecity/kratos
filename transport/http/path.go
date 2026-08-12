package http

import (
	"reflect"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/go-kratos/kratos/v3/encoding/form"
)

var pathTemplateParamRE = regexp.MustCompile(`{([.\w]+)(=[^{}]*)?}`)

// BuildPathOption configures path construction.
type BuildPathOption func(*buildPathOptions)

type buildPathOptions struct {
	queryParams bool
	omitFields  []string
}

// WithQueryParams appends request fields that are not bound in the path as query parameters.
func WithQueryParams() BuildPathOption {
	return func(o *buildPathOptions) {
		o.queryParams = true
	}
}

// WithOmitFields excludes fields from generated query parameters.
func WithOmitFields(fields ...string) BuildPathOption {
	return func(o *buildPathOptions) {
		o.omitFields = append(o.omitFields, fields...)
	}
}

/*
BuildPath 是 Kratos HTTP 客户端中用于将 Protobuf 消息自动映射为 HTTP 请求路径 + Query 参数的核心函数。它实现了 Google AIP-127 定义的 HTTP/gRPC 转码规范。

下面逐层拆解：

整体流程

输入: pathTemplate="/v1/users/{user_id}/posts/{post_id}"
      msg=&ListPostsRequest{UserId: "123", PostId: "456", PageSize: 10, Filter: "active"}
      opts=[WithQueryParams()]

输出: "/v1/users/123/posts/456?page_size=10&filter=active"
       ↑______↑               ↑_____________________↑
       路径参数替换                  剩余字段变为 query string

核心逻辑分三步：① 提取路径参数 → ② 替换模板占位符 → ③ 剩余字段拼 query

正则解析路径模板

var pathTemplateParamRE = regexp.MustCompile({([.w]+)(=[^{}]*)?})

匹配 {key} 或 {key=pattern} 两种形式：
模板片段   matches[1] (key)   matches[2] (pattern)   说明
{user_id}   user_id   ""   简单参数

{name=users}   name   =users/   带自定义 pattern（AIP-127）

{parent=projects/locations/}   parent   =projects/locations/   嵌套资源名

pattern 部分在 BuildPath 中被忽略（仅做 key 提取），因为客户端只需要知道"这个占位符对应哪个字段"，pattern 校验是服务端的事。

逐步执行分析

Step 0: Nil 安全检查
if msg == nil || (reflect.ValueOf(msg).Kind() == reflect.Pointer && reflect.ValueOf(msg).IsNil()) {
    return pathTemplate
}

msg 为空时直接返回原始模板，避免 panic。注意这里用了反射检查指针是否为 nil，因为 msg any 接口本身不会是 nil（typed nil 问题）。

Step 1: 将消息编码为 URL Values
queryParams, _ := form.EncodeValues(msg)

调用 Kratos 的 form encoder，把 protobuf message 的所有字段扁平化为 map[string][]string：
{
  "user_id":   ["123"],
  "post_id":   ["456"],
  "page_size": ["10"],
  "filter":    ["active"],
}

这一步同时处理了嵌套消息（foo.bar.baz）、repeated 字段、field mask 等复杂类型。

Step 2: 替换路径参数并记录已用字段
pathParams := make(map[string]struct{})
path = pathTemplateParamRE.ReplaceAllStringFunc(pathTemplate, func(in string) string {
    matches := pathTemplateParamRE.FindStringSubmatch(in)
    key := matches[1]
    pathParams[key] = struct{}{}        // 记录"这个字段已经用在路径里了"
    return queryParams.Get(key)          // 从编码结果中取值替换
})

替换后：
path = "/v1/users/123/posts/456"
pathParams = {"user_id": {}, "post_id": {}}

Step 3: 决定是否拼接 Query String

这里有两条分支：

分支 A：未启用 WithQueryParams()
if !options.queryParams {
    if v, ok := msg.(proto.Message); ok {
        if query := form.EncodeFieldMask(v.ProtoReflect()); query != "" {
            return path + "?" + query
        }
    }
    return path
}

默认行为：不把剩余字段加到 query
唯一例外：如果消息包含 google.protobuf.FieldMask 字段，则只把 field_mask 编码为 query（这是 gRPC transcoding 规范要求，用于 PATCH 语义）

分支 B：启用了 WithQueryParams()
for key := range pathParams {
    delete(queryParams, key)           // ① 移除已在路径中使用的字段
}
omitQueryParams(queryParams, options.omitFields)  // ② 移除显式排除的字段
if query := queryParams.Encode(); query != "" {
    path += "?" + query                // ③ 拼接剩余字段
}

omitQueryParams 的作用

func omitQueryParams(values map[string][]string, fields []string) {
    for _, field := range fields {
        delete(values, field)              // 精确删除
        prefix := field + "."
        for key := range values {
            if strings.HasPrefix(key, prefix) {  // 前缀删除（嵌套字段）
                delete(values, key)
            }
        }
    }
}

支持两种排除模式：
用法   效果
WithOmitFields("page_size")   删除 page_size

WithOmitFields("filter")   删除 filter、filter.name、filter.status 等所有嵌套子字段

典型场景：某些字段已经在 header/body 中传递，不应重复出现在 query 中。

设计要点总结
设计决策   原因
路径参数和 query 参数共用同一个 EncodeValues 结果   避免对消息编码两次，保证一致性

默认不追加 query params   遵循 gRPC transcoding 规范：只有显式声明 body: "*" 之外的字段才进 query

FieldMask 特殊处理   PATCH 请求必须通过 query 传递 field_mask，即使没开 WithQueryParams

用 map[string]struct{} 记录 pathParams   O(1) 查找，零值内存开销

正则预编译为包级变量   避免每次调用重新编译正则

omitFields 支持前缀匹配   protobuf 嵌套字段被 form encoder 扁平化为 a.b.c 格式，需要级联删除

这个函数本质上是一个 Protobuf → HTTP Request 的单向转码器，与服务端的反向转码（HTTP → Protobuf）配合，实现了 gRPC API 的 RESTful 暴露。
*/

// BuildPath builds an HTTP request path from a path template and request message.
func BuildPath(pathTemplate string, msg any, opts ...BuildPathOption) string {
	if msg == nil || (reflect.ValueOf(msg).Kind() == reflect.Pointer && reflect.ValueOf(msg).IsNil()) {
		return pathTemplate
	}

	options := buildPathOptions{}
	for _, opt := range opts {
		opt(&options)
	}

	queryParams, _ := form.EncodeValues(msg)
	pathParams := make(map[string]struct{})
	path := pathTemplate
	if strings.ContainsRune(pathTemplate, '{') {
		path = pathTemplateParamRE.ReplaceAllStringFunc(pathTemplate, func(in string) string {
			matches := pathTemplateParamRE.FindStringSubmatch(in)
			key := matches[1]
			pathParams[key] = struct{}{}
			return queryParams.Get(key)
		})
	}

	if !options.queryParams {
		if v, ok := msg.(proto.Message); ok {
			if query := form.EncodeFieldMask(v.ProtoReflect()); query != "" {
				return path + "?" + query
			}
		}
		return path
	}
	if len(queryParams) > 0 {
		for key := range pathParams {
			delete(queryParams, key)
		}
		omitQueryParams(queryParams, options.omitFields)
		if query := queryParams.Encode(); query != "" {
			path += "?" + query
		}
	}
	return path
}

func omitQueryParams(values map[string][]string, fields []string) {
	for _, field := range fields {
		if field == "" {
			continue
		}
		delete(values, field)
		prefix := field + "."
		for key := range values {
			if strings.HasPrefix(key, prefix) {
				delete(values, key)
			}
		}
	}
}
