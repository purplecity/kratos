package p2c

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v3/selector"
	"github.com/go-kratos/kratos/v3/selector/node/ewma"
)

/*
这段代码实现了一个全局单次强制探测机制。

它的核心含义是：当某个低权重节点长时间未被选中时，系统允许“强制选中它一次”以刷新其指标；但在任意时刻，整个 Balancer 只允许一个 goroutine 执行这种强制选中，其他并发请求即使也遇到了需要强制选中的节点，也必须放弃强制机会，走正常的权重比较逻辑。

下面逐层拆解：

为什么需要强制选中？

P2C 算法天然存在 “富者愈富、穷者愈穷” 的问题：

节点 A 权重高 → 被频繁选中 → EWMA 指标持续更新 → 权重保持准确
节点 B 权重低（比如刚恢复、或之前出错导致 success 下降）→ 几乎不被选中 → EWMA 指标停滞在旧的差值上 → 权重一直低 → 永远无法翻身

forcePick = time.Second * 3 就是为了解决这个问题：如果 upc（未选中的那个节点）已经超过 3 秒没被 Pick 过，说明它的指标可能已经过期了，需要给它一次机会去采集新数据。

CompareAndSwap(false, true) 的作用：全局互斥

s.picked 是一个 atomic.Bool，属于 Balancer 级别（不是 Node 级别），是所有并发 Pick 调用共享的。

if upc.PickElapsed() > forcePick && s.picked.CompareAndSwap(false, true) {
    defer s.picked.Store(false)
    pc = upc  // 强制选中低权重节点
}

场景   CAS 结果   行为
当前没有人在做强制选中   false→true 成功   获得强制权，选中 upc，Done 后释放

已有另一个 goroutine 正在强制选中   false→true 失败（读到 true）   放弃强制，正常按权重选 pc

为什么不能每个 goroutine 都独立强制选中？

假设 100 个 goroutine 同时发现同一个冷门节点超过 3s 未被选中，如果没有这个 CAS：
100 个请求全部涌向该节点 → 瞬间形成流量尖刺
该节点可能刚恢复还没完全就绪，被瞬时打垮
EWMA 采集到的是被击穿时的异常延迟，反而得到更差的指标

CAS 保证了：每轮强制探测只有一个请求过去，像探针一样轻柔地试探，而不是洪水般涌入。

defer s.picked.Store(false) 的作用：自动释放

强制选中的请求完成后（Done 回调执行完毕，或者 Pick 返回后调用方使用完节点），通过 defer 将 picked 重置为 false，允许下一次强制探测。

⚠️ 注意时序
defer 是在 Pick() 函数返回时执行的，不是在 Done 回调中执行的。这意味着：
强制锁的持有时间 = Pick() 方法的执行时间（极短，微秒级）
不是整个请求的生命周期
这是故意设计的：强制锁只是为了防止多个 goroutine 在同一瞬间都对同一个冷门节点发起强制选中。一旦 Pick 完成、节点已被选定，锁就应该立即释放，让后续的强制探测机会留给其他可能的冷门节点。请求本身的 EWMA 指标更新由 Done 回调独立完成，不需要持锁。

完整流程示例

t=0s: 节点 B 最后一次被选中
t=3.1s: Goroutine-1 Pick → prePick 选中 A(权重80) 和 B(权重20)
        → B.PickElapsed() = 3.1s > 3s ✅
        → CAS(false→true) 成功 ✅
        → pc = B（强制选中）
        → defer Store(false) 注册
        → Pick() 返回 B

t=3.1s (几乎同时): Goroutine-2 Pick → prePick 也选中 A 和 B
        → B.PickElapsed() > 3s ✅
        → CAS(false→true) 失败 ❌（Goroutine-1 还没 defer 释放）
        → 跳过强制逻辑
        → 正常比较权重 → pc = A
        → Pick() 返回 A

t=3.1s+: Goroutine-1 的 Pick() 返回，defer 执行 → picked = false
        → 下一次强制探测窗口打开

📌 一句话总结

s.picked.CompareAndSwap(false, true) + defer Store(false) 构成了一个轻量级的全局信号量，确保在高并发下，对长期未被选中节点的强制探测同一时刻最多只有一个请求执行，既解决了 EWMA 指标停滞问题，又避免了强制探测本身引发的流量风暴。
*/

const (
	forcePick = time.Second * 3
	// Name is p2c(Pick of 2 choices) balancer name
	Name = "p2c"
)

var _ selector.Balancer = (*Balancer)(nil)

// Option is p2c builder option.
type Option func(o *options)

// options is p2c builder options
type options struct{}

// New creates a p2c selector.
func New(opts ...Option) selector.Selector {
	return NewBuilder(opts...).Build()
}

// Balancer is p2c selector.
type Balancer struct {
	mu     sync.Mutex
	r      *rand.Rand
	picked atomic.Bool
}

// choose two distinct nodes.
func (s *Balancer) prePick(nodes []selector.WeightedNode) (nodeA selector.WeightedNode, nodeB selector.WeightedNode) {
	s.mu.Lock()
	a := s.r.IntN(len(nodes))
	b := s.r.IntN(len(nodes) - 1)
	s.mu.Unlock()
	if b >= a {
		b = b + 1
	}
	nodeA, nodeB = nodes[a], nodes[b]
	return
}

// Pick pick a node.
func (s *Balancer) Pick(_ context.Context, nodes []selector.WeightedNode) (selector.WeightedNode, selector.DoneFunc, error) {
	if len(nodes) == 0 {
		return nil, nil, selector.ErrNoAvailable
	}
	if len(nodes) == 1 {
		done := nodes[0].Pick()
		return nodes[0], done, nil
	}

	var pc, upc selector.WeightedNode
	nodeA, nodeB := s.prePick(nodes)
	// meta.Weight is the weight set by the service publisher in discovery
	if nodeB.Weight() > nodeA.Weight() {
		pc, upc = nodeB, nodeA
	} else {
		pc, upc = nodeA, nodeB
	}

	// If the failed node has never been selected once during forceGap, it is forced to be selected once
	// Take advantage of forced opportunities to trigger updates of success rate and delay
	if upc.PickElapsed() > forcePick && s.picked.CompareAndSwap(false, true) {
		defer s.picked.Store(false)
		pc = upc
	}
	done := pc.Pick()
	return pc, done, nil
}

// NewBuilder returns a selector builder with p2c balancer
func NewBuilder(opts ...Option) selector.Builder {
	var option options
	for _, opt := range opts {
		opt(&option)
	}
	return &selector.DefaultBuilder{
		Balancer: &Builder{},
		Node:     &ewma.Builder{},
	}
}

// Builder is p2c builder
type Builder struct{}

// Build creates Balancer
func (b *Builder) Build() selector.Balancer {
	return &Balancer{r: rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0))}
}
