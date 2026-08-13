package grpc

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/metadata"

	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"
	"github.com/go-kratos/kratos/v3/transport"
)

/*
google.golang.org/grpc/balancer 是 gRPC Go 官方提供的客户端负载均衡框架。

注意关键词：客户端。它和你之前问的 Kratos selector 解决的是同一个问题（“这次请求发给谁”），但它是 gRPC 原生生态的实现，而 Kratos selector 是 Kratos 框架自己的实现。

它具体干嘛？

当你的 gRPC Client 连接一个服务名（如 dns:///my-service）时，底层会发生：

Resolver (DNS/ETCD) → 返回 []Address
        ↓
balancer.Balancer ← 接收地址列表更新
        ↓
Picker.Pick() ← 每次 RPC 调用时，决定用哪个 Address
        ↓
建立/复用 SubConn → 发出请求

balancer 包定义了这套机制的所有核心接口：
接口/类型   职责
Balancer   管理后端地址列表，创建/关闭 SubConn，响应状态变更

Picker   轻量级选择器，每次 RPC 调用时被触发，返回一个 SubConn

SubConn   到某个具体后端的连接抽象（可能包含多条 TCP 连接）

ClientConn   Balancer 与 gRPC ClientConn 交互的回调接口

Builder   工厂模式，按名称注册和创建 Balancer 实例

gRPC 内置了哪些 Balancer？
名称   策略   说明
pick_first   顺序尝试   默认策略，连上第一个可用的就 stick 住

round_robin   轮询   在所有 READY 的 SubConn 间均匀分配

base   基础框架   不是直接用，而是作为自定义 Balancer 的嵌入基类

⚠️ 关键认知：它和 Kratos Selector 的关系

这是最容易混淆的点。在 Kratos 体系中，这两者通常不会同时生效：

┌─────────────────────────────────────────────────┐
│           Kratos gRPC Client                     │
│                                                  │
│  Discovery → Resolver → Subset → Selector(P2C)  │  ← Kratos 自己的链路
│       ↓                                          │
│  直接拿到具体 Address，绕过 gRPC balancer         │
│       ↓                                          │
│  grpc.Dial("passthrough:///<具体IP:Port>")        │  ← 用 passthrough 禁用 gRPC LB
└─────────────────────────────────────────────────┘

Kratos 的做法：自己做服务发现 + 负载均衡，然后把已经选好的具体地址通过 passthrough resolver 喂给 gRPC。此时 gRPC 的 balancer 实际上被旁路了（用的是默认的 pick_first，因为只有一个地址）。
gRPC 原生做法：把服务名传给 gRPC，让 gRPC 内部的 resolver + balancer 全权负责。

什么时候你会直接用到 grpc/balancer？
场景   说明
不用 Kratos，纯 gRPC 项目   需要自定义 LB 策略（如加权、一致性哈希、优先级故障转移）

对接 xDS / Service Mesh   Envoy/Istio 通过 xDS 下发 LB 策略，gRPC 用 xds balancer 接收

编写 gRPC 通用库/中间件   比如写一个通用的熔断 balancer，供多个项目复用

理解 Kratos 设计决策   知道 gRPC 原生 LB 的局限性，才能理解为什么 Kratos 要自建 selector

为什么 Kratos 不直接用 gRPC Balancer？
gRPC Balancer 的局限   Kratos Selector 的优势
与 gRPC 传输层深度耦合，无法复用到 HTTP   协议无关，HTTP/gRPC 共用一套 P2C/WRR

Picker 接口过于简单，难以实现复杂反馈机制   P2C 基于实时延迟/成功率做自适应决策

地址更新和选择逻辑耦合在 Balancer 内部   Discovery / Subset / Selector 三层正交解耦

生态封闭，只服务于 gRPC Go   可独立演进、测试、替换

📌 一句话总结

grpc/balancer 是 gRPC 官方的客户端负载均衡插件框架。如果你用 Kratos，你几乎不需要直接接触它——Kratos 用自己的 selector 替代了它的职责；如果你脱离 Kratos 写纯 gRPC 服务且需要高级 LB 策略，它就是你要扩展的核心入口。




这个文件是 Kratos Selector 与 gRPC 原生 Balancer 机制之间的“桥梁”。

它完美回答了你上一个问题中留下的悬念：“既然 Kratos 有自己的 Selector，为什么还要关心 grpc/balancer？”

答案是：Kratos 并没有绕过 gRPC Balancer，而是把自己“伪装”成了一个 gRPC Balancer 注入进去。

核心设计意图

gRPC 客户端的负载均衡是不可绕过的内部机制。即使你传入了具体 IP，gRPC 内部仍然会走 Picker.Pick() 流程。与其对抗这个机制，Kratos 选择接管它：

gRPC 原生 Balancer 框架
        ↓ 注册名为 "selector" 的自定义 Balancer
balancerBuilder.Build()
        ↓ 将 gRPC 的 SubConn 转换为 selector.Node
        ↓ 用 Kratos Selector (P2C/WRR/Random) 做实际决策
balancerPicker.Pick()
        ↓ 返回选中的 SubConn + Done 回调
gRPC 发出请求 → 收到响应 → 调用 Done() 反馈指标

这样既保留了 gRPC 连接管理的完整性（SubConn 复用、健康检查、重连），又让 Kratos 的高级 LB 算法得以生效。

逐段解析

init() — 全局注册
func init() {
    b := base.NewBalancerBuilder(
        balancerName,           // 名称: "selector"
        &balancerBuilder{...},  // Picker 构建器
        base.Config{HealthCheck: true}, // 启用 gRPC 原生健康检查
    )
    balancer.Register(b)
}

在包导入时自动执行
向 gRPC 全局注册一个名为 "selector" 的 Balancer
用户在创建 gRPC Client 时通过 grpc.WithDefaultServiceConfig({"loadBalancingConfig": [{"selector": {}}]}) 激活它
HealthCheck: true 意味着 gRPC 会自动对每个 SubConn 发送 health check RPC，只有 READY 的连接才会进入 ReadySCs

balancerBuilder.Build() — 地址转换层
这是整个桥接最关键的逻辑：
gRPC 概念   Kratos 概念   转换方式
base.PickerBuildInfo.ReadySCs   []selector.Node   遍历所有就绪的 SubConn

Address.Attributes["rawServiceInstance"]   *registry.ServiceInstance   从 gRPC Address 的属性中提取原始服务实例元数据

SubConn   grpcNode.subConn   绑定到 Node 上，Pick 时能找回对应的连接

⚠️ 关键细节：rawServiceInstance
gRPC 的 Address 本身只有 IP:Port。但 Kratos Selector（尤其是 P2C）需要 Metadata、Version、Weight 等信息来做决策。这些信息是在 Resolver 阶段被塞进 Address.Attributes 的，这里再取出来还原为完整的 selector.Node。没有这一步，Selector 就退化为普通的轮询。

balancerPicker.Pick() — 运行时选择 + 反馈闭环

func (p *balancerPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
    // ① 提取 NodeFilter（支持按条件过滤节点，如按版本、标签）
    filters := gtr.NodeFilters()

    // ② 调用 Kratos Selector 做真正的选择
    n, done, err := p.selector.Select(info.Ctx, selector.WithNodeFilter(filters...))

    // ③ 返回 SubConn + Done 回调
    return balancer.PickResult{
        SubConn: n.(*grpcNode).subConn,
        Done: func(di balancer.DoneInfo) {
            done(info.Ctx, selector.DoneInfo{
                Err:           di.Err,
                BytesSent:     di.BytesSent,
                BytesReceived: di.BytesReceived,
                ReplyMD:       Trailer(di.Trailer),
            })
        },
    }, nil
}

这里的 Done 回调是 P2C 算法的生命线：
gRPC 在每次 RPC 完成后（无论成功失败）都会调用 Done
Kratos 借此收集延迟、错误率、字节数等实时指标
P2C 正是依赖这些反馈来动态调整权重，实现自适应负载均衡

如果没有这个 Done 回调，P2C 就失去了反馈信号，退化为盲选。

整体架构定位

把这个文件和之前讨论的概念串起来：

┌──────────────────────────────────────────────────────┐
│                  Kratos gRPC Client                   │
│                                                       │
│  Discovery ──→ Resolver ──→ Subset                    │
│       │              │         │                      │
│       │         塞入 Attributes  过滤后地址             │
│       ▼              ▼         ▼                      │
│  grpc.Dial() ──→ gRPC Balancer Framework              │
│                       │                               │
│              balancerBuilder.Build()                  │
│              (SubConn → selector.Node)                 │
│                       │                               │
│              balancerPicker.Pick()                    │
│              (Kratos Selector 做决策)                   │
│                       │                               │
│              Done() 反馈指标 → P2C 更新权重             │
│                       │                               │
│                  SubConn 发出请求                      │
└──────────────────────────────────────────────────────┘

📌 一句话总结

balancer.go 是 Kratos 将自有 Selector 体系无缝嵌入 gRPC 原生负载均衡框架的适配器。它让 Kratos 在不破坏 gRPC 连接管理、健康检查、重试等底层能力的前提下，用 P2C 等高级算法替换了 gRPC 默认的 pick_first/round_robin 策略，同时通过 Done 回调建立了完整的性能反馈闭环。


*/

const (
	balancerName = "selector"
)

var (
	_ base.PickerBuilder = (*balancerBuilder)(nil)
	_ balancer.Picker    = (*balancerPicker)(nil)
)

func init() {
	b := base.NewBalancerBuilder(
		balancerName,
		&balancerBuilder{
			builder: selector.GlobalSelector(),
		},
		base.Config{HealthCheck: true},
	)
	balancer.Register(b)
}

type balancerBuilder struct {
	builder selector.Builder
}

// Build creates a grpc Picker.
func (b *balancerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	if len(info.ReadySCs) == 0 {
		// Block the RPC until a new picker is available via UpdateState().
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	nodes := make([]selector.Node, 0, len(info.ReadySCs))
	for conn, info := range info.ReadySCs {
		ins, _ := info.Address.Attributes.Value("rawServiceInstance").(*registry.ServiceInstance)
		nodes = append(nodes, &grpcNode{
			Node:    selector.NewNode("grpc", info.Address.Addr, ins),
			subConn: conn,
		})
	}
	p := &balancerPicker{
		selector: b.builder.Build(),
	}
	p.selector.Apply(nodes)
	return p
}

// balancerPicker is a grpc picker.
type balancerPicker struct {
	selector selector.Selector
}

// Pick pick instances.
func (p *balancerPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var filters []selector.NodeFilter
	if tr, ok := transport.FromClientContext(info.Ctx); ok {
		if gtr, ok := tr.(*Transport); ok {
			filters = gtr.NodeFilters()
		}
	}

	n, done, err := p.selector.Select(info.Ctx, selector.WithNodeFilter(filters...))
	if err != nil {
		return balancer.PickResult{}, err
	}

	return balancer.PickResult{
		SubConn: n.(*grpcNode).subConn,
		Done: func(di balancer.DoneInfo) {
			done(info.Ctx, selector.DoneInfo{
				Err:           di.Err,
				BytesSent:     di.BytesSent,
				BytesReceived: di.BytesReceived,
				ReplyMD:       Trailer(di.Trailer),
			})
		},
	}, nil
}

// Trailer is a grpc trailer MD.
type Trailer metadata.MD

// Get get a grpc trailer value.
func (t Trailer) Get(k string) string {
	v := metadata.MD(t).Get(k)
	if len(v) > 0 {
		return v[0]
	}
	return ""
}

type grpcNode struct {
	selector.Node
	subConn balancer.SubConn
}
