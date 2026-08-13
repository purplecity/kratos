package circuitbreaker

import (
	"context"

	"github.com/go-kratos/kratos/v3/errors"
	internalbreaker "github.com/go-kratos/kratos/v3/internal/circuitbreaker"
	"github.com/go-kratos/kratos/v3/internal/group"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
)

/*
⚡ 熔断器（Circuit Breaker）到底是什么？

用最通俗的话说：熔断器就是一个“自动保险丝”。

当你的服务调用下游（比如数据库、第三方API）频繁失败时，熔断器会主动切断请求，直接返回错误，而不是让请求继续发过去超时等待。等下游恢复了，它再自动尝试放行。

它的核心目的不是“修复错误”，而是 防止雪崩：避免一个下游故障拖垮整个上游服务（线程池耗尽、连接数打满、内存OOM）。

🔌 三种状态（核心模型）

        失败率超阈值          超时后
 [CLOSED] ──────────► [OPEN] ───────► [HALF-OPEN]
    ▲                    │                  │
    │                    │                  │
    └────────────────────┘                  │
         探测成功                            │
                                            │
         探测失败 ◄─────────────────────────┘
         (回到 OPEN)

状态   行为   类比
Closed（关闭）   正常放行所有请求，后台统计失败率   保险丝正常导通

Open（打开）   直接拒绝所有请求，不调用下游，立即返回 ErrNotAllowed   保险丝烧断了

Half-Open（半开）   只放行一个探测请求，成功→Closed，失败→Open   换个新保险丝试一下

💡 关键认知
Open 状态下请求被拒绝是毫秒级本地判断，不涉及任何网络IO。这就是熔断器能保护上游的根本原因——把"等待下游超时30秒"变成了"本地判断3纳秒"。

🔍 结合你贴的代码逐行解读

按 Operation 隔离熔断器

breaker := opt.group.Get(info.Operation())

group.Group 是一个 懒加载的并发安全 Map。每个 Operation()（通常是 /api/v1/users 这样的接口路径）拥有独立的熔断器实例。

这意味着：/users 接口挂了不会导致 /orders 也被熔断。故障被隔离在单个接口粒度。

Allow()：门禁检查

if err := breaker.Allow(); err != nil {
    breaker.MarkFailed()  // ⬅️ 注意这行！
    return nil, ErrNotAllowed
}

Allow() 返回 error → 当前处于 Open 状态，请求被拒绝
被拒绝后仍然 MarkFailed()：这是 Kratos 的一个精妙设计。被熔断拒绝的请求也算作"失败"，这会持续推高失败计数，使得熔断器的恢复窗口不断后移。如果下游真的还没好，就不会因为"一段时间没有真实请求进来"而误判为已恢复。

MarkFailed / MarkSuccess：结果反馈

if err != nil && (errors.IsInternalServer(err) || errors.IsServiceUnavailable(err) || errors.IsGatewayTimeout(err)) {
    breaker.MarkFailed()
} else {
    breaker.MarkSuccess()
}

只有特定的服务端错误才算失败：
✅ 500 Internal Server Error → 下游真的出问题了
✅ 503 Service Unavailable → 下游过载
✅ 504 Gateway Timeout → 下游响应太慢
❌ 400 Bad Request → 这是调用方传参错了，跟下游健康无关
❌ 404 Not Found → 资源不存在，不是故障
❌ context.Canceled → 调用方主动取消，不算下游失败

⚠️ 这个过滤逻辑极其重要
如果把 4xx 也算作失败，那么一个传参错误的客户端就能把你的熔断器触发，导致所有正常请求都被拒绝。这是很多自研熔断器踩过的坑。

🆚 熔断 vs 限流 vs 重试

很多人混淆这三者，它们的职责完全不同：
机制   解决的问题   触发条件   动作
熔断   下游故障导致的雪崩   失败率超阈值   快速失败，保护上游

限流   上游流量过大打垮下游   QPS 超限   拒绝多余请求，保护下游

重试   偶发性瞬时失败   单次请求失败   重新发起，提高成功率

⚠️ 熔断 + 重试 = 灾难
如果熔断器已经 Open，重试只会浪费 CPU 反复命中 ErrNotAllowed。正确做法：先熔断，熔断放行后再重试。Kratos 中间件链的顺序应该是 CircuitBreaker → Retry → Handler。

📌 一句话总结

熔断器是一个基于失败率的自适应开关：下游正常时透明放行，下游异常时毫秒级切断请求保护自身，下游恢复后自动试探性重连。你贴的这段代码是它在 Kratos Client 侧的标准实现，核心亮点在于 按接口隔离、拒绝时持续计失败、只对真正的服务端错误熔断。
*/

// ErrNotAllowed is request failed due to circuit breaker triggered.
var ErrNotAllowed = errors.New(503, "CIRCUITBREAKER", "request failed due to circuit breaker triggered")

// CircuitBreaker is a circuit breaker.
type CircuitBreaker = internalbreaker.CircuitBreaker

// Option is circuit breaker option.
type Option func(*options)

// WithBreakerFactory configures a factory used to lazily create one circuit breaker per operation.
func WithBreakerFactory(factory func() CircuitBreaker) Option {
	return func(o *options) {
		if factory == nil {
			return
		}
		o.group = group.NewGroup(factory)
	}
}

type options struct {
	group *group.Group[CircuitBreaker]
}

// Client circuitbreaker middleware will return errBreakerTriggered when the circuit
// breaker is triggered and the request is rejected directly.
func Client(opts ...Option) middleware.Middleware {
	opt := &options{
		group: group.NewGroup(func() CircuitBreaker {
			return internalbreaker.NewBreaker()
		}),
	}
	for _, o := range opts {
		o(opt)
	}
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (any, error) {
			info, _ := transport.FromClientContext(ctx)
			breaker := opt.group.Get(info.Operation())
			if err := breaker.Allow(); err != nil {
				// rejected
				// NOTE: when client reject requests locally,
				// continue to add counter let the drop ratio higher.
				breaker.MarkFailed()
				return nil, ErrNotAllowed
			}
			// allowed
			reply, err := handler(ctx, req)
			if err != nil && (errors.IsInternalServer(err) || errors.IsServiceUnavailable(err) || errors.IsGatewayTimeout(err)) {
				breaker.MarkFailed()
			} else {
				breaker.MarkSuccess()
			}
			return reply, err
		}
	}
}
