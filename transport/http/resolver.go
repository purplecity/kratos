package http

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/go-kratos/kratos/v3/internal/endpoint"
	"github.com/go-kratos/kratos/v3/internal/subset"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"
)

/*
这是一个非常敏锐的问题。乍看之下，filtered 里已经有了完整的服务实例信息（*registry.ServiceInstance），再转成 selector.Node 似乎是多此一举。

但实际上，这两者在 Kratos 架构中承担着完全不同的职责边界。这个转换是必须的，原因如下：

核心原因：解耦“服务发现”与“负载均衡”
registry.ServiceInstance 属于 服务发现层 (Discovery)。它包含的是注册中心下发的原始元数据（如 metadata、version、weight、所有 endpoints 等）。
selector.Node 属于 负载均衡/选择器层 (Selector)。它只关心“如何路由请求”，只需要协议、最终地址和权重等路由要素。

如果让 Selector 直接依赖 registry.ServiceInstance，就意味着你的负载均衡算法被绑定死在了特定的注册中心数据结构上。通过转换为 Node，Kratos 实现了：无论底层是 Consul、Nacos 还是 K8s，上层的选择器算法（P2C、WRR、Random）都使用统一的接口。

关键操作：Endpoint 的提取与标准化
注意看这段代码：
for _, ins := range filtered {
    // ⚠️ 第二次调用 ParseEndpoint
    ept, _ := endpoint.ParseEndpoint(ins.Endpoints, endpoint.Scheme(schemeHTTP, !r.insecure))
    nodes = append(nodes, selector.NewNode(schemeHTTP, ept, ins))
}

一个 ServiceInstance 可能注册了多个 Endpoints（例如同时暴露了 gRPC grpc://10.0.0.1:9000 和 HTTP http://10.0.0.1:8000）。
filtered 阶段只是确认“该实例有可用的 HTTP endpoint”。
Node 构建阶段则是精确提取出那个具体的 HTTP 地址字符串，并将其封装为 Node。Selector 在做负载均衡时，拿到的是可以直接用于建立连接的 address，而不是自己去遍历解析 endpoints 列表。

Subset 分片后的节点重建
注意 subset.Subset 是在 filtered 之后、nodes 构建之前执行的：
if r.subsetSize != 0 {
    filtered = subset.Subset(r.selectorKey, filtered, r.subsetSize)
}
// subset 过滤后，只对剩余的实例构建 Node
nodes := make([]selector.Node, 0, len(filtered))

Subset 算法需要基于 ServiceInstance 的 ID/元数据进行一致性哈希分片。分片完成后，只有被选中的子集才需要转换为 Node 并 Apply 给 Rebalancer。这避免了为已被 Subset 淘汰的实例创建无用的 Node 对象。

为什么 ParseEndpoint 被调用了两次？
你可能会注意到 update 方法中 ParseEndpoint 在 filter 循环和 node 构建循环中各调用了一次。这确实是一个可以优化的点（可以在第一次解析时将 ept 缓存到临时结构中），但当前设计选择了逻辑清晰优先于微小性能开销：
第一次：纯过滤，判断有效性
第二次：纯构建，提取地址

由于 update 只在服务实例变更时触发（非热路径），这里的重复解析开销完全可以接受。

总结对比
维度   filtered ([]*ServiceInstance)   nodes ([]selector.Node)
所属抽象层   Registry / Discovery   Selector / LoadBalancer

包含信息   全量元数据、所有协议的 endpoints   单一协议的具体地址 + 路由权重

消费者   Subset 分片算法、Endpoint 过滤器   Rebalancer.Apply() → P2C/WRR 等算法

生命周期   仅在 update 函数内临时存在   被 Rebalancer 持有，直到下次 Apply

设计目的   “哪些实例是可用的？”   “向哪个具体地址发请求？”

一句话总结：filtered 回答的是“谁可用”，nodes 回答的是“连哪里”。从 ServiceInstance 到 Node 的转换，本质上是从“服务注册语义”到“网络路由语义”的翻译过程，这是 Kratos 分层架构的核心体现。
*/

// Target is resolver target
type Target struct {
	Scheme    string
	Authority string
	Endpoint  string
}

func parseTarget(endpoint string, insecure bool) (*Target, error) {
	if !strings.Contains(endpoint, "://") {
		if insecure {
			endpoint = schemeHTTP + "://" + endpoint
		} else {
			endpoint = schemeHTTPS + "://" + endpoint
		}
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	target := &Target{Scheme: u.Scheme, Authority: u.Host}
	if len(u.Path) > 1 {
		target.Endpoint = u.Path[1:]
	}
	return target, nil
}

type resolver struct {
	rebalancer selector.Rebalancer

	target      *Target
	watcher     registry.Watcher
	selectorKey string
	subsetSize  int

	insecure bool
}

func newResolver(ctx context.Context, discovery registry.Discovery, target *Target,
	rebalancer selector.Rebalancer, block, insecure bool, subsetSize int,
) (*resolver, error) {
	// this is new resolver
	watcher, err := discovery.Watch(ctx, target.Endpoint)
	if err != nil {
		return nil, err
	}
	r := &resolver{
		target:      target,
		watcher:     watcher,
		rebalancer:  rebalancer,
		insecure:    insecure,
		selectorKey: uuid.New().String(),
		subsetSize:  subsetSize,
	}
	if block {
		done := make(chan error, 1)
		go func() {
			for {
				services, err := watcher.Next()
				if err != nil {
					done <- err
					return
				}
				if r.update(services) {
					done <- nil
					return
				}
			}
		}()
		select {
		case err := <-done:
			if err != nil {
				stopErr := watcher.Stop()
				if stopErr != nil {
					log.Error("failed to stop http client watcher", "target", target, "error", stopErr)
				}
				return nil, err
			}
		case <-ctx.Done():
			log.Error("http client watch service reached context deadline", "target", target)
			stopErr := watcher.Stop()
			if stopErr != nil {
				log.Error("failed to stop http client watcher", "target", target, "error", stopErr)
			}
			return nil, ctx.Err()
		}
	}
	go func() {
		for {
			services, err := watcher.Next()
			if err != nil {
				if errors.Is(err, context.Canceled) {
					return
				}
				log.Error("http client watch service got unexpected error", "target", target, "error", err)
				time.Sleep(time.Second)
				continue
			}
			r.update(services)
		}
	}()
	return r, nil
}

func (r *resolver) update(services []*registry.ServiceInstance) bool {
	filtered := make([]*registry.ServiceInstance, 0, len(services))
	for _, ins := range services {
		ept, err := endpoint.ParseEndpoint(ins.Endpoints, endpoint.Scheme(schemeHTTP, !r.insecure))
		if err != nil {
			log.Error("failed to parse discovery endpoint", "target", r.target, "endpoints", ins.Endpoints, "error", err)
			continue
		}
		if ept == "" {
			continue
		}
		filtered = append(filtered, ins)
	}
	if r.subsetSize != 0 {
		filtered = subset.Subset(r.selectorKey, filtered, r.subsetSize)
	}
	nodes := make([]selector.Node, 0, len(filtered))
	for _, ins := range filtered {
		ept, _ := endpoint.ParseEndpoint(ins.Endpoints, endpoint.Scheme(schemeHTTP, !r.insecure))
		nodes = append(nodes, selector.NewNode(schemeHTTP, ept, ins))
	}

	if len(nodes) == 0 {
		log.Warn("[http resolver] zero endpoint found, refused to write", "endpoint", r.target.Endpoint, "nodes", nodes)
		return false
	}
	r.rebalancer.Apply(nodes)
	return true
}

func (r *resolver) Close() error {
	return r.watcher.Stop()
}
