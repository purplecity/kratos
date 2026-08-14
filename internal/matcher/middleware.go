package matcher

import (
	"sort"
	"strings"

	"github.com/go-kratos/kratos/v3/middleware"
)

/*
Match 方法是整个中间件匹配器的核心查询入口。它的作用是根据传入的操作路径（operation，通常是 gRPC method 或 HTTP path），返回一个按优先级组装好的中间件切片。

🔍 执行流程逐行解析

func (m *matcher) Match(operation string) []middleware.Middleware {
    // ① 初始化结果切片，预分配容量为默认中间件数量
    ms := make([]middleware.Middleware, 0, len(m.defaults))

    // ② 第一层：无条件加载全局默认中间件
    if len(m.defaults) > 0 {
        ms = append(ms, m.defaults...)
    }

    // ③ 第二层：精确匹配（最高优先级）
    if next, ok := m.matches[operation]; ok {
        return append(ms, next...)
    }

    // ④ 第三层：前缀匹配（次优先级）
    for _, prefix := range m.prefix {
        if strings.HasPrefix(operation, prefix) {
            return append(ms, m.matches[prefix]...)
        }
    }

    // ⑤ 兜底：无任何特定匹配，仅返回默认中间件
    return ms
}

📊 三层匹配优先级
优先级   匹配方式   触发条件   示例
🥇 最高   精确匹配   operation 与 Add() 注册的 key 完全相等   /user.v1.UserService/GetUser

🥈 次高   前缀匹配   operation 以某个带 * 注册的前缀开头   /user.v1.UserService/* → 前缀 /user.v1.UserService/

🥉 最低   全局默认   以上都不命中   Use() 注册的中间件

⚠️ 关键设计：短路返回
精确匹配和前缀匹配一旦命中，立即 return。不会同时叠加多个特定规则的中间件。这意味着如果你同时注册了 /foo/bar 和 /foo/*，当 operation 为 /foo/bar 时，只有精确匹配的中间件生效，前缀匹配的不会叠加。

🔗 与 Add() 的联动机制

要理解 Match，必须结合 Add 中前缀的处理逻辑：

// Add 中注册 "/api/v1/*" 时：
selector = strings.TrimSuffix("/api/v1", "")  // → "/api/v1/"
m.prefix = append(m.prefix, "/api/v1/")           // 存入前缀列表
sort.Slice(...)                                    // 降序排列
m.matches["/api/v1/"] = ms                        // 用去掉*后的key存中间件

Match 中的前缀遍历正是依赖这个约定：
m.prefix 里存的是**去掉 * 后的纯前缀字符串**
m.matches[prefix] 能用这个纯前缀作为 key 取到对应的中间件
strings.HasPrefix(operation, prefix) 实现了通配符语义

📏 为什么 prefix 要降序排序？

sort.Slice(m.prefix, func(i, j int) bool {
    return m.prefix[i] > m.prefix[j]  // 降序：长的在前
})

这保证了最长前缀优先匹配。假设注册了两个前缀规则：

/api/v1/user/*     → 前缀 "/api/v1/user/"
/api/v1/*          → 前缀 "/api/v1/"

降序排列后：["/api/v1/user/", "/api/v1/"]

当 operation 为 /api/v1/user/profile 时：
先检查 /api/v1/user/ → ✅ 命中，返回对应中间件
/api/v1/ 不再检查（已短路返回）

如果不排序，可能先命中更短的 /api/v1/，导致更精确的规则被跳过。这是典型的"最长前缀匹配"策略，与路由器的路由表查找原理一致。

💡 使用示例推演

m := New()
m.Use(loggingMiddleware)                          // 全局
m.Add("/user.v1.UserService/GetUser", authMW)     // 精确
m.Add("/user.v1.UserService/*", rateLimitMW)      // 前缀
m.Add("/admin/*", adminAuthMW)                    // 前缀

operation   匹配过程   返回的中间件链
/user.v1.UserService/GetUser   默认 ✅ → 精确 ✅ → return   [logging, auth]

/user.v1.UserService/ListUsers   默认 ✅ → 精确 ❌ → 前缀 /user.v1.UserService/ ✅ → return   [logging, rateLimit]

/admin/dashboard   默认 ✅ → 精确 ❌ → 前缀 /admin/ ✅ → return   [logging, adminAuth]

/healthz   默认 ✅ → 精确 ❌ → 前缀全❌ → return ms   [logging]

⚠️ 潜在注意点

并发安全：matcher 结构体没有加锁。Add 和 Match 不应在不同 goroutine 中并发调用。通常做法是在服务启动阶段完成所有 Add，运行时只读调用 Match。
空 operation：如果传入空字符串，精确匹配会查 matches[""]，前缀匹配对所有非空前缀都返回 false，最终只返回默认中间件。行为是安全的。
内存分配：每次 Match 都会 make 一个新切片并拷贝默认中间件。在高频调用场景下可考虑缓存或对象池优化，但在一般微服务请求处理中开销可忽略。
*/

// Matcher is a middleware matcher.
type Matcher interface {
	Use(ms ...middleware.Middleware)
	Add(selector string, ms ...middleware.Middleware)
	Match(operation string) []middleware.Middleware
}

// New new a middleware matcher.
func New() Matcher {
	return &matcher{
		matches: make(map[string][]middleware.Middleware),
	}
}

type matcher struct {
	prefix   []string
	defaults []middleware.Middleware
	matches  map[string][]middleware.Middleware
}

func (m *matcher) Use(ms ...middleware.Middleware) {
	m.defaults = ms
}

func (m *matcher) Add(selector string, ms ...middleware.Middleware) {
	if strings.HasSuffix(selector, "*") {
		selector = strings.TrimSuffix(selector, "*")
		m.prefix = append(m.prefix, selector)
		// sort the prefix:
		//  - /foo/bar
		//  - /foo
		sort.Slice(m.prefix, func(i, j int) bool {
			return m.prefix[i] > m.prefix[j]
		})
	}
	m.matches[selector] = ms
}

func (m *matcher) Match(operation string) []middleware.Middleware {
	ms := make([]middleware.Middleware, 0, len(m.defaults))
	if len(m.defaults) > 0 {
		ms = append(ms, m.defaults...)
	}
	if next, ok := m.matches[operation]; ok {
		return append(ms, next...)
	}
	for _, prefix := range m.prefix {
		if strings.HasPrefix(operation, prefix) {
			return append(ms, m.matches[prefix]...)
		}
	}
	return ms
}
