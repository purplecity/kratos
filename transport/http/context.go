package http

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"

	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
)

var _ Context = (*wrapper)(nil)

// Context is an HTTP Context.
type Context interface {
	context.Context
	Vars() url.Values
	Query() url.Values
	Form() url.Values
	Header() http.Header
	Request() *http.Request
	Response() http.ResponseWriter
	Middleware(middleware.Handler) middleware.Handler
	Bind(any) error
	BindVars(any) error
	BindQuery(any) error
	BindForm(any) error
	Returns(any, error) error
	Result(int, any) error
	JSON(int, any) error
	XML(int, any) error
	String(int, string) error
	Blob(int, string, []byte) error
	Stream(int, string, io.Reader) error
	Reset(http.ResponseWriter, *http.Request)
}

type responseWriter struct {
	code int
	w    http.ResponseWriter
}

func (w *responseWriter) reset(res http.ResponseWriter) {
	w.w = res
	w.code = http.StatusOK
}
func (w *responseWriter) Header() http.Header        { return w.w.Header() }
func (w *responseWriter) WriteHeader(statusCode int) { w.code = statusCode }
func (w *responseWriter) Write(data []byte) (int, error) {
	w.w.WriteHeader(w.code)
	return w.w.Write(data)
}
func (w *responseWriter) Unwrap() http.ResponseWriter { return w.w }

type wrapper struct {
	router *Router
	req    *http.Request
	res    http.ResponseWriter
	w      responseWriter
}

func (c *wrapper) Header() http.Header {
	return c.req.Header
}

func (c *wrapper) Vars() url.Values {
	raws := mux.Vars(c.req)
	vars := make(url.Values, len(raws))
	for k, v := range raws {
		vars[k] = []string{v}
	}
	return vars
}

func (c *wrapper) Form() url.Values {
	if err := c.req.ParseForm(); err != nil {
		return url.Values{}
	}
	return c.req.Form
}

func (c *wrapper) Query() url.Values {
	return c.req.URL.Query()
}
func (c *wrapper) Request() *http.Request        { return c.req }
func (c *wrapper) Response() http.ResponseWriter { return c.res }
func (c *wrapper) Middleware(h middleware.Handler) middleware.Handler {
	if tr, ok := transport.FromServerContext(c.req.Context()); ok {
		return middleware.Chain(c.router.srv.middleware.Match(tr.Operation())...)(h)
	}
	return middleware.Chain(c.router.srv.middleware.Match(c.req.URL.Path)...)(h)
}
func (c *wrapper) Bind(v any) error      { return c.router.srv.decBody(c.req, v) }
func (c *wrapper) BindVars(v any) error  { return c.router.srv.decVars(c.req, v) }
func (c *wrapper) BindQuery(v any) error { return c.router.srv.decQuery(c.req, v) }
func (c *wrapper) BindForm(v any) error  { return bindForm(c.req, v) }
func (c *wrapper) Returns(v any, err error) error {
	if err != nil {
		return err
	}
	return c.router.srv.enc(&c.w, c.req, v)
}

/*
这两个方法分别代表了 Kratos HTTP 框架中两种截然不同的响应模式：通用编码响应与原始流式响应。下面逐一深入解析。

Result(code int, v any) error — 通用结构化响应

func (c *wrapper) Result(code int, v any) error {
    c.w.WriteHeader(code)
    return c.router.srv.enc(&c.w, c.req, v)
}

核心职责
将任意 Go 对象 v 按照服务端预配置的编码器自动序列化后写入响应体，同时设置状态码。它是业务 handler 中最常用的返回方式。

关键细节
要素   说明
c.w.WriteHeader(code)   调用的是自定义 responseWriter.WriteHeader，仅记录状态码到 w.code，不会立即发送 HTTP header

c.router.srv.enc   这是 Server 初始化时注册的统一编码函数，内部会根据请求的 Accept / Content-Type 头自动选择 JSON、XML、Protobuf 等编码器

&c.w   传入的是包装后的 writer，当 enc 内部调用 Write(data) 时，才会真正触发 w.w.WriteHeader(w.code) + w.w.Write(data)（延迟写入）

返回值   如果编码或写入失败，返回 error；上层 middleware 可据此做错误处理/日志记录

典型使用场景
// handler 中只需关心业务数据，不关心序列化格式
func (s *UserService) GetUser(ctx context.Context, reqGetUserRequest) (UserReply, error) {
    user := &UserReply{Name: "Alice", Age: 30}
    // 客户端 Accept: application/json → JSON 输出
    // 客户端 Accept: application/protobuf → Protobuf 输出
    return http.ResultFromContext(ctx).Result(200, user)
}

为什么用 c.w 而不是 c.res？
因为 c.w 是 responseWriter 包装器，它实现了状态码延迟写入机制：
直接写 c.res.WriteHeader() 会立即发送 header，后续无法修改
通过 c.w，状态码先缓存，直到第一次 Write() 时才真正发出
这允许编码器在写入 body 前还能调整 header（如设置 Content-Length）

Stream(code int, contentType string, rd io.Reader) error — 原始流式响应

func (c *wrapper) Stream(code int, contentType string, rd io.Reader) error {
    c.res.Header().Set("Content-Type", contentType)
    c.res.WriteHeader(code)           // ⚠️ 注意：这里直接用 c.res，不是 c.w
    _, err := io.Copy(c.res, rd)
    return err
}

核心职责
将一个 io.Reader 的内容原封不动地以指定 Content-Type 流式传输给客户端，完全绕过服务端的编码器体系。

关键细节
要素   说明
contentType 参数   必须由调用方显式指定，框架不做任何推断

c.res.WriteHeader(code)   直接使用原始 ResponseWriter，立即发送状态码和 header，不走延迟写入

io.Copy(c.res, rd)   内部使用 32KB buffer 循环 Read→Write，内存占用恒定，适合大文件/长连接

无编码参与   srv.enc 完全不介入，数据逐字节透传

错误语义   io.Copy 返回的 error 可能是读取源失败或写入连接断开，调用方需自行判断

典型使用场景
// 场景1: 文件下载
func DownloadHandler(ctx http.Context) error {
    f, _ := os.Open("/data/report.csv")
    defer f.Close()
    return ctx.Stream(200, "text/csv; charset=utf-8", f)
}

// 场景2: SSE (Server-Sent Events)
func SSEHandler(ctx http.Context) error {
    return ctx.Stream(200, "text/event-stream", eventReader)
}

// 场景3: 代理转发上游响应
func ProxyHandler(ctx http.Context) error {
    resp, _ := httpClient.Get(upstreamURL)
    return ctx.Stream(resp.StatusCode, resp.Header.Get("Content-Type"), resp.Body)
}

两者核心对比
维度   Result   Stream
数据来源   Go 结构体 / map / proto.Message   io.Reader（文件、网络连接、pipe 等）

序列化   由 srv.enc 自动编码   无编码，原始字节透传

Content-Type   由编码器根据协商结果自动设置   调用方手动指定

Writer   c.w（延迟写入，支持 header 后调整）   c.res（立即写入，不可逆）

内存模型   编码后一次性写入（小对象）或分块   32KB buffer 流式拷贝，内存恒定

适用协议   REST API、gRPC-Web、JSON/XML/Proto   文件下载、SSE、视频流、反向代理

FieldMask 支持   ✅ 自动过滤字段   ❌ 不适用

设计哲学

Result 体现的是 "声明式" 思想：handler 只描述"我要返回什么数据"，框架负责"怎么序列化、用什么格式"。这与 Kratos 的 protobuf-first 理念一致。
Stream 体现的是 "逃生舱" 思想：当数据本身就不是结构化对象（二进制流、第三方响应体），或者需要精确控制传输行为时，跳过框架抽象，直接操作底层 HTTP 原语。

两者互补，覆盖了从标准 API 响应到非标流式传输的完整场景。
*/

func (c *wrapper) Result(code int, v any) error {
	c.w.WriteHeader(code)
	return c.router.srv.enc(&c.w, c.req, v)
}

func (c *wrapper) JSON(code int, v any) error {
	c.res.Header().Set("Content-Type", contentTypeJSON)
	c.res.WriteHeader(code)
	return json.NewEncoder(c.res).Encode(v)
}

func (c *wrapper) XML(code int, v any) error {
	c.res.Header().Set("Content-Type", "application/xml")
	c.res.WriteHeader(code)
	return xml.NewEncoder(c.res).Encode(v)
}

func (c *wrapper) String(code int, text string) error {
	c.res.Header().Set("Content-Type", "text/plain")
	c.res.WriteHeader(code)
	_, err := c.res.Write([]byte(text))
	if err != nil {
		return err
	}
	return nil
}

func (c *wrapper) Blob(code int, contentType string, data []byte) error {
	c.res.Header().Set("Content-Type", contentType)
	c.res.WriteHeader(code)
	_, err := c.res.Write(data)
	if err != nil {
		return err
	}
	return nil
}

/*
在 Kratos 中使用 Stream 的核心原则是：你负责提供数据源（io.Reader）和内容类型，框架负责搬运字节。

下面给出三个从简单到进阶的典型实战示例：

基础用法：文件下载
这是最常见的场景，直接将本地文件流式传输给客户端，无论文件多大内存占用都只有 32KB。

import (
    "os"
    "github.com/go-kratos/kratos/v3/transport/http"
)

func DownloadHandler(ctx http.Context) error {
    filePath := "/data/reports/monthly.csv"

    // 打开文件（注意不要在这里 defer Close，见下方说明）
    f, err := os.Open(filePath)
    if err != nil {
        return ctx.Result(404, map[string]string{"error": "file not found"})
    }

    // ⚠️ 关键：关闭操作应该在 Stream 返回之后执行
    // 因为 io.Copy 是同步阻塞的，Stream 返回时数据已经写完
    defer f.Close()

    return ctx.Stream(200, "text/csv; charset=utf-8", f)
}

⚠️ 避坑提醒
千万不要在调用 Stream 之前就提前读取或消费了 io.Reader，否则 io.Copy 拿到的将是剩余部分甚至空流。同时确保 Reader 在被完全消费前不会被关闭。

进阶用法：SSE (Server-Sent Events) 实时推送
用于 AI 对话流式输出、实时通知等场景。SSE 要求禁用缓冲并持续写入。

import (
    "fmt"
    "io"
    "time"
    "github.com/go-kratos/kratos/v3/transport/http"
)

func SSEHandler(ctx http.Context) error {
    // 创建一个管道，writer 端生产数据，reader 端传给 Stream
    pr, pw := io.Pipe()

    // 在后台协程中持续生成事件
    go func() {
        defer pw.Close() // 发送完毕或出错时必须关闭，否则 Stream 会永久阻塞
        for i := 0; i < 5; i++ {
            msg := fmt.Sprintf("data: message %dnn", i)
            if _, err := pw.Write([]byte(msg)); err != nil {
                return // 客户端断开连接时 Write 会报错，直接退出
            }
            time.Sleep(time.Second)
        }
    }()

    // text/event-stream 是 SSE 的标准 MIME 类型
    return ctx.Stream(200, "text/event-stream", pr)
}

高级用法：反向代理透传上游响应
将另一个服务的响应体原封不动转发给当前客户端，避免将整个响应加载到内存。

import (
    "net/http"
    kratosHttp "github.com/go-kratos/kratos/v3/transport/http"
)

func ProxyHandler(ctx kratosHttp.Context) error {
    upstreamURL := "https://api.example.com/large-dataset"

    resp, err := http.DefaultClient.Get(upstreamURL)
    if err != nil {
        return ctx.Result(502, map[string]string{"error": "upstream unreachable"})
    }
    // ⚠️ 必须关闭 Body，防止连接泄漏
    defer resp.Body.Close()

    // 直接透传上游的状态码、Content-Type 和响应体
    return ctx.Stream(resp.StatusCode, resp.Header.Get("Content-Type"), resp.Body)
}

💡 核心注意事项总结
关注点   正确做法   错误做法
Reader 生命周期   defer Close() 放在 Stream 调用之后；由于 io.Copy 是同步的，返回时已读完   在另一个 goroutine 中提前 Close，或在 Stream 前就消费了 Reader

Content-Type   显式传入准确的 MIME 类型   传空字符串或依赖框架猜测（Stream 不会自动推断）

状态码时机   Stream 内部会立即写入 header，调用前无法再修改   试图在 Stream 之后再调用 WriteHeader

错误处理   检查 Stream 返回值，区分"读源失败"和"写客户端失败"   忽略 error，导致客户端收到截断数据却无感知

并发安全   每个请求使用独立的 Reader   多个请求共享同一个未加锁的 Reader

一句话记忆：Result 是给框架一个对象让它帮你序列化，Stream 是你自己准备好一条水管（io.Reader），框架只负责拧开水龙头把水送到客户端。
*/

func (c *wrapper) Stream(code int, contentType string, rd io.Reader) error {
	c.res.Header().Set("Content-Type", contentType)
	c.res.WriteHeader(code)
	_, err := io.Copy(c.res, rd)
	return err
}

func (c *wrapper) Reset(res http.ResponseWriter, req *http.Request) {
	c.w.reset(res)
	c.res = res
	c.req = req
}

func (c *wrapper) Deadline() (time.Time, bool) {
	if c.req == nil {
		return time.Time{}, false
	}
	return c.req.Context().Deadline()
}

func (c *wrapper) Done() <-chan struct{} {
	if c.req == nil {
		return nil
	}
	return c.req.Context().Done()
}

func (c *wrapper) Err() error {
	if c.req == nil {
		return context.Canceled
	}
	return c.req.Context().Err()
}

func (c *wrapper) Value(key any) any {
	if c.req == nil {
		return nil
	}
	return c.req.Context().Value(key)
}
