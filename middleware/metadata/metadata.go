package metadata

import (
	"context"
	"strings"

	"github.com/go-kratos/kratos/v3/metadata"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
)

/*
这个中间件是 Kratos 框架中实现 元数据（Metadata）透传与隔离 的核心组件。它解决了微服务调用链中一个经典难题：哪些信息需要随请求链路自动传递下去，哪些信息只应该在当前服务内部使用？

通过 x-md-global- 和 x-md-local- 两种前缀约定，它在 Server 端完成“接收与分类”，在 Client 端完成“过滤与转发”，实现了元数据的精细化管控。

一、核心设计思想：Global vs Local
类型   前缀   行为   典型用途
Global   x-md-global-   ✅ 跨服务自动透传   TraceID、UserID、TenantID、灰度标签

Local   x-md-local-   ❌ 仅当前服务可见，不透传   内部调试标记、临时路由参数、鉴权中间产物

Constants   自定义   ✅ 每次请求固定注入   服务版本号、机房标识、环境名

💡 为什么需要区分？
如果不加区分地透传所有 Header，会导致：
安全风险：内部鉴权 Token 被意外转发到下游第三方服务
性能浪费：大量无用 Header 占用网络带宽和序列化开销
语义污染：上游的临时标记干扰下游的正常逻辑

二、Server 端中间件详解

func Server(opts ...Option) middleware.Middleware {
    options := &options{
        prefix: []string{"x-md-"}, // ⬅️ 注意：Server 端默认匹配 "x-md-"
    }
    // ...
    return func(handler middleware.Handler) middleware.Handler {
        return func(ctx context.Context, req any) (reply any, err error) {
            tr, ok := transport.FromServerContext(ctx)
            if !ok {
                return handler(ctx, req) // 非 transport 上下文，直接放行
            }

            md := options.md.Clone()     // ① 先克隆常量元数据作为基底
            header := tr.RequestHeader() // ② 从传输层提取请求头
            for _, k := range header.Keys() {
                if options.hasPrefix(k) { // ③ 只接收 x-md- 前缀的 Key
                    for _, v := range header.Values(k) {
                        md.Add(k, v)      // ④ 追加到元数据（支持多值）
                    }
                }
            }
            ctx = metadata.NewServerContext(ctx, md) // ⑤ 写入 Server Context
            return handler(ctx, req)
        }
    }
}

关键细节

Server 端前缀是 x-md-：这意味着 Server 会同时接收 Global 和 Local 两种元数据。这是合理的——当前服务需要看到上游传来的所有 x-md- 信息，包括 Local 的（因为 Local 只是"不再往下传"，不是"当前服务不能用"）。
options.md.Clone()：常量元数据作为底层基底，请求头中的同名 Key 会通过 Add 追加而非覆盖，保证常量始终存在。
大小写不敏感：hasPrefix 内部做了 strings.ToLower(k)，兼容 HTTP/2 全小写和 HTTP/1.1 混合大小写的 Header。
多值支持：使用 header.Values(k) + md.Add() 而非 md.Set()，正确处理同一个 Key 出现多次的情况（如多个 Cookie、多个 Tag）。

三、Client 端中间件详解

Client 端的逻辑比 Server 复杂得多，因为它要合并三个来源并过滤透传范围：

func Client(opts ...Option) middleware.Middleware {
    options := &options{
        prefix: []string{"x-md-global-"}, // ⬅️ 注意：Client 端只透传 Global
    }
    // ...
    return func(handler middleware.Handler) middleware.Handler {
        return func(ctx context.Context, req any) (reply any, err error) {
            tr, ok := transport.FromClientContext(ctx)
            if !ok {
                return handler(ctx, req)
            }

            header := tr.RequestHeader()

            // ① Constants：无条件注入
            for k, vList := range options.md {
                for _, v := range vList {
                    header.Add(k, v)
                }
            }

            // ② Client Context：手动设置的元数据，全部注入
            if md, ok := metadata.FromClientContext(ctx); ok {
                for k, vList := range md {
                    for _, v := range vList {
                        header.Add(k, v)
                    }
                }
            }

            // ③ Server Context → Client 透传：仅 Global 前缀
            if md, ok := metadata.FromServerContext(ctx); ok {
                for k, vList := range md {
                    if options.hasPrefix(k) { // ⬅️ 只有 x-md-global- 才透传
                        for _, v := range vList {
                            header.Add(k, v)
                        }
                    }
                }
            }

            return handler(ctx, req)
        }
    }
}

三层数据来源的优先级与职责

┌─────────────────────────────────────────────────┐
│              Client 发出请求时的 Header           │
├─────────────────────────────────────────────────┤
│ 第3层: ServerContext 中 x-md-global-*  ← 自动透传 │
│ 第2层: ClientContext 中的所有 Key      ← 业务显式设置│
│ 第1层: Constants 常量                  ← 固定注入   │
└─────────────────────────────────────────────────┘

⚠️ **重要：x-md-local-* 在 Client 端被静默丢弃**
Server 端接收了 x-md-local- 并存入 ServerContext，但 Client 端在透传时只检查 x-md-global- 前缀。这意味着 Local 元数据天然止步于当前服务，不会泄漏到下游。这正是该设计的精髓。

四、完整调用链示例

假设服务 A → 服务 B → 服务 C：

用户请求 → 服务A (Server Middleware)
           ↓ 收到 Header: x-md-global-uid=123, x-md-local-debug=true
           ↓ ServerContext = {x-md-global-uid:123, x-md-local-debug:true}

服务A → 服务B (Client Middleware)
           ↓ 透传: x-md-global-uid=123 ✅
           ↓ 丢弃: x-md-local-debug=true ❌
           ↓ 注入: x-md-global-version=v2.1 (Constants)

服务B (Server Middleware)
           ↓ ServerContext = {x-md-global-uid:123, x-md-global-version:v2.1}
           ↓ 注意：x-md-local-debug 已不存在

服务B → 服务C (Client Middleware)
           ↓ 透传: x-md-global-uid=123, x-md-global-version=v2.1 ✅

五、设计亮点与注意事项

零侵入性：业务代码只需通过 metadata.FromServerContext(ctx) 读取，完全不感知传输协议（HTTP/gRPC）。切换协议无需改业务代码。
安全默认值：Client 端默认只透传 x-md-global-，防止开发者误将敏感信息设为普通 Header 后被意外传播。想透传必须显式加前缀。
Clone() 防污染：Server 端对常量元数据做 Clone，避免多个并发请求共享同一个 map 导致数据竞争或交叉污染。
transport.FromXxxContext 的防御性检查：当中间件被用在非 transport 场景（如本地单元测试、消息队列消费者）时，优雅降级而不是 panic。
⚠️ 潜在陷阱：Client 端的 FromClientContext 是全量注入的（不做前缀过滤）。如果你在业务代码中往 ClientContext 塞了一个不带 x-md- 前缀的 Key，它也会被发到下游。前缀过滤只对 ServerContext 的自动透传生效。这是有意为之——业务显式设置的元数据被视为"开发者明确意图"，框架不做额外限制。

📌 一句话总结

这个中间件通过 Server 端宽进（x-md-）、Client 端严出（x-md-global-） 的非对称前缀策略，配合 Constants 常量注入和 ClientContext 显式设置，构建了一套安全、自动、分层的元数据传播机制，是 Kratos 微服务治理基础设施的关键一环。
*/

// Option is metadata option.
type Option func(*options)

type options struct {
	prefix []string
	md     metadata.Metadata
}

func (o *options) hasPrefix(key string) bool {
	k := strings.ToLower(key)
	for _, prefix := range o.prefix {
		if strings.HasPrefix(k, prefix) {
			return true
		}
	}
	return false
}

// WithConstants with constant metadata key value.
func WithConstants(md metadata.Metadata) Option {
	return func(o *options) {
		o.md = md
	}
}

// WithPropagatedPrefix with propagated key prefix.
func WithPropagatedPrefix(prefix ...string) Option {
	return func(o *options) {
		o.prefix = prefix
	}
}

// Server is middleware server-side metadata.
func Server(opts ...Option) middleware.Middleware {
	options := &options{
		prefix: []string{"x-md-"}, // x-md-global-, x-md-local
	}
	for _, o := range opts {
		o(options)
	}
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (reply any, err error) {
			tr, ok := transport.FromServerContext(ctx)
			if !ok {
				return handler(ctx, req)
			}

			md := options.md.Clone()
			header := tr.RequestHeader()
			for _, k := range header.Keys() {
				if options.hasPrefix(k) {
					for _, v := range header.Values(k) {
						md.Add(k, v)
					}
				}
			}
			ctx = metadata.NewServerContext(ctx, md)
			return handler(ctx, req)
		}
	}
}

// Client is middleware client-side metadata.
func Client(opts ...Option) middleware.Middleware {
	options := &options{
		prefix: []string{"x-md-global-"},
	}
	for _, o := range opts {
		o(options)
	}
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req any) (reply any, err error) {
			tr, ok := transport.FromClientContext(ctx)
			if !ok {
				return handler(ctx, req)
			}

			header := tr.RequestHeader()
			// x-md-local-
			for k, vList := range options.md {
				for _, v := range vList {
					header.Add(k, v)
				}
			}
			if md, ok := metadata.FromClientContext(ctx); ok {
				for k, vList := range md {
					for _, v := range vList {
						header.Add(k, v)
					}
				}
			}
			// x-md-global-
			if md, ok := metadata.FromServerContext(ctx); ok {
				for k, vList := range md {
					if options.hasPrefix(k) {
						for _, v := range vList {
							header.Add(k, v)
						}
					}
				}
			}
			return handler(ctx, req)
		}
	}
}
