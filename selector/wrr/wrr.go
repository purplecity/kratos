package wrr

import (
	"context"
	"sync"

	"github.com/go-kratos/kratos/v3/selector"
	"github.com/go-kratos/kratos/v3/selector/node/direct"
)

/*
这个文件实现了 Kratos 中的 WRR（Weighted Round Robin，加权轮询） 负载均衡算法。

与之前讨论的 P2C/EWMA 不同，WRR 是一种确定性、无状态反馈的调度算法。它不关心节点的实时延迟或成功率，只根据预设或计算出的“权重”按比例分配流量。代码中明确注释了这是 Nginx 的平滑加权轮询算法（Smooth Weighted Round-Robin） 的 Go 语言实现。

一、核心算法原理：Nginx 平滑 WRR

普通的加权轮询（如 A:5, B:3, C:2）会产生 AAAAABBBCC 这样的序列，导致节点 A 瞬间承受大量请求。Nginx 的平滑算法通过引入 currentWeight 变量，将请求均匀打散，生成类似 ABACABAACA 的平滑序列。

算法伪代码
对于每次 Pick：
遍历所有节点：
   currentWeight[node] += effectiveWeight[node]
选出 currentWeight 最大的节点作为 selected
currentWeight[selected] -= totalWeight
返回 selected

💡 数学本质
每个节点的 currentWeight 可以理解为“累积的调度欠账”。权重越高的节点，每轮累积得越快，被选中的频率就越高。减去 totalWeight 是为了让所有节点的 currentWeight 在一个周期内归零重置，保证长期比例精确等于权重比。

二、代码逐段解析

数据结构

type Balancer struct {
    mu            sync.Mutex                // 保护并发安全（WRR 是有状态的）
    currentWeight map[string]float64        // 每个节点的当前累积权重
    lastNodes     []selector.WeightedNode   // 上一次 Pick 时的节点列表快照
}

⚠️ 关键认知：为什么 WRR 需要锁，而 EWMA/P2C 不需要？

EWMA/P2C：每个 Node 自己维护自己的指标（atomic），Pick 时只是读取+比较，无需全局协调。
WRR：currentWeight 是全局共享状态，每次 Pick 都会修改它。多个 goroutine 同时 Pick 必须串行化，否则权重累加会错乱。这也是 WRR 在高并发场景下性能不如 P2C 的根本原因。

equalNodes() — 节点变更检测

func equalNodes(a, b []selector.WeightedNode) bool {
    if len(a) != len(b) { return false }
    aMap := make(map[string]bool, len(a))
    for _, node := range a { aMap[node.Address()] = true }
    for _, node := range b {
        if !aMap[node.Address()] { return false }
    }
    return true
}

为什么需要这个？

服务发现是动态的，节点可能随时增减。如果节点列表变了但 currentWeight 没清理：
已下线的节点：其 currentWeight 永远留在 map 中 → 内存泄漏
新上线的节点：没有历史 currentWeight，初始值为 0 → 第一轮不会被选中（这其实是合理的冷启动行为）

这个函数在每次 Pick 时 O(n) 检查节点列表是否变化，仅在变化时才触发清理。相比每次都重建 map，这是一个实用的性能优化。

Pick() — 核心调度逻辑

func (p *Balancer) Pick(_ context.Context, nodes []selector.WeightedNode) (...) {
    p.mu.Lock()
    defer p.mu.Unlock()

    // ① 节点变更处理
    if !equalNodes(p.lastNodes, nodes) {
        p.lastNodes = make([]selector.WeightedNode, len(nodes))
        copy(p.lastNodes, nodes)
        currentNodes := make(map[string]bool, len(nodes))
        for _, node := range nodes { currentNodes[node.Address()] = true }
        for address := range p.currentWeight {
            if !currentNodes[address] { delete(p.currentWeight, address) }
        }
    }

    // ② Nginx SWRR 核心循环
    var totalWeight float64
    var selected selector.WeightedNode
    var selectWeight float64

    for _, node := range nodes {
        totalWeight += node.Weight()
        cwt := p.currentWeight[node.Address()]
        cwt += node.Weight()                    // 累加有效权重
        p.currentWeight[node.Address()] = cwt
        if selected == nil || selectWeight < cwt {
            selectWeight = cwt
            selected = node                     // 选累积权重最大的
        }
    }
    p.currentWeight[selected.Address()] = selectWeight - totalWeight  // 减去总权重

    d := selected.Pick()
    return selected, d, nil
}

执行示例（A:5, B:3, C:2）
轮次   累加后 currentWeight   选中   减去 totalWeight(10) 后
1   A=5, B=3, C=2   A   A=-5, B=3, C=2

2   A=0, B=6, C=4   B   A=0, B=-4, C=4

3   A=5, B=-1, C=6   C   A=5, B=-1, C=-4

4   A=10, B=2, C=-2   A   A=0, B=2, C=-2

5   A=5, B=5, C=0   A/B   (取决于遍历顺序)

...   ...   ...   ...

10 轮后，A 被选 5 次，B 被选 3 次，C 被选 2 次，且分布均匀。

三、WRR vs P2C(EWMA) 对比
维度   WRR   P2C + EWMA
决策依据   静态/外部权重   实时延迟 + 成功率 + 在途数

自适应能力   ❌ 无，节点变慢不会自动降权   ✅ 全自动反馈调节

并发性能   🔒 全局互斥锁   🔓 无锁 atomic 操作

确定性   ✅ 相同权重产生相同序列   ❌ 随机选两个，结果不确定

适用场景   网关路由、灰度发布、按规格分配   微服务间调用、后端实例均衡

Kratos 默认   否   ✅ 是

四、设计细节与注意事项

node.Weight() 的来源：WRR 本身不计算权重，它依赖 WeightedNode.Weight() 的实现。如果搭配 direct.Builder（本文件默认），权重来自服务注册中心的 metadata；如果搭配 ewma.Node，权重就是 EWMA 实时计算的动态值。WRR 只是一个调度器，权重来源是可插拔的。
sync.Mutex 的性能瓶颈：在高 QPS 场景下，这把锁会成为热点。这也是为什么 Kratos 默认推荐 P2C 而非 WRR。如果你的服务 QPS > 10K，应谨慎使用 WRR。
equalNodes 的 O(n) 开销：每次 Pick 都要遍历节点列表做比较。当节点数很多（>1000）时，这个开销不可忽略。生产环境中 WRR 更适合节点数较少的场景（如网关到上游集群）。
DoneFunc 的作用：WRR 的 Done 回调实际上不做任何权重更新（不像 EWMA 那样反馈指标）。它只是透传了 selected.Pick() 返回的 Done，保持接口一致性。

📌 一句话总结

这个文件是 Nginx 平滑加权轮询算法的 Go 实现，它通过 currentWeight 累积机制将请求按权重比例均匀分散到各节点。它是一种简单、确定、可预测的调度策略，适合权重由外部指定且不需要自适应反馈的场景，但在高并发和动态环境下的表现不如 Kratos 默认的 P2C+EWMA 方案。





你说得非常对，Nginx 平滑 WRR 算法确实依赖遍历顺序。当两个节点的 currentWeight 相等时，谁在切片中排前面，谁就会被选中。

但这不是 Bug，而是该算法的固有特性（Feature）。下面详细解释为什么这种“顺序依赖”在实际生产中是可以接受的，以及 Kratos 是如何处理它的。

为什么顺序依赖不影响“长期公平性”？

WRR 的核心保证是：在一个完整周期（totalWeight 次请求）内，每个节点被选中的次数严格等于其权重值。

以 A:2, B:2 为例（totalWeight=4）：
轮次   累加后   选中   减4后   说明
1   A=2, B=2   A (先遍历)   A=-2, B=2   A 赢在顺序

2   A=0, B=4   B   A=0, B=0   B 累积更高

3   A=2, B=2   A (先遍历)   A=-2, B=2   A 又赢在顺序

4   A=0, B=4   B   A=0, B=0   周期结束

结果：A 被选 2 次，B 被选 2 次。比例精确 1:1。

💡 关键认知
顺序依赖只影响 “谁先谁后”，不影响 “总共多少次”。WRR 保证的是长期比例精确，而不是短期随机均匀。如果你需要短期随机均匀，应该用 P2C 或 Random。

什么情况下顺序依赖会成为问题？
场景   是否有问题   原因
权重各不相同   ❌ 几乎无影响   currentWeight 相等的概率极低

权重相同 + 节点少   ⚠️ 有轻微偏差   排前面的节点总是先被选中，形成固定模式

权重相同 + 大量同权节点   ⚠️ 有明显偏差   前部节点持续优先，后部节点“饥饿感”更强

动态权重频繁变化   ❌ 无影响   权重变化会打破相等状态

Kratos 的实际缓解机制

虽然算法本身有顺序依赖，但在 Kratos 的实际运行中，这个问题被大幅弱化：

① 节点列表本身是不稳定的
服务发现返回的节点列表每次都可能顺序不同（DNS 解析、ETCD watch 事件、网络抖动都会导致重排序）。这意味着“谁排前面”本身就在不断变化，顺序依赖被自然打散。

② EWMA 动态权重几乎不会相等
如果你用的是 ewma.Node（Kratos 默认），权重是浮点数实时计算的：
weight = float64(health * 1e4) / float64(load)

两个节点的 lag、inflight、success 完全相同的概率趋近于零。浮点权重不相等 → currentWeight 不会相等 → 遍历顺序无关紧要。

③ WRR 的典型使用场景本身就是有序的
WRR 最常见的用途是：
灰度发布：canary:10, stable:90 → 权重不同
按规格分配：8C16G:8, 4C8G:4 → 权重不同
优先级路由：primary:100, fallback:1 → 权重不同

在这些场景中，权重天然不等，顺序依赖根本不会触发。

如果你真的需要消除顺序依赖

有两种改法：

方案 A：Pick 前 Shuffle（简单但有开销）
// 在 Pick 开头加一行
rand.Shuffle(len(nodes), func(i, j int) { nodes[i], nodes[j] = nodes[j], nodes[i] })

⚠️ 缺点：破坏了 WRR 的“平滑”特性，退化为加权随机；且 Shuffle 本身有 O(n) 开销。

方案 B：相等时随机选择（推荐）
if selected == nil || selectWeight < cwt ||
   (selectWeight == cwt && rand.Intn(2) == 0) {
    selectWeight = cwt
    selected = node
}

✅ 仅在权重真正相等时才引入随机，既保持了平滑性，又消除了顺序偏差。

📌 总结

你的观察完全正确：WRR 确实依赖遍历顺序。但在实践中：
它只影响调度时序，不影响长期比例公平性
动态权重 + 不稳定的节点列表使相等概率极低
WRR 的典型使用场景中权重天然不等

所以 Kratos 选择了保持 Nginx 原始实现的简洁性，没有额外处理顺序依赖。如果你的场景确实是大量同权节点且对顺序敏感，可以在 Pick 中加入相等时的随机打破逻辑。


好的，我们用 A:5, B:3, C:2 这个经典例子，结合 Kratos WRR 的 Pick 代码逐行执行。

初始状态

// currentWeight 初始为空 map
p.currentWeight = map[string]float64{}
// nodes 切片顺序固定为 [A, B, C]
nodes = [A(weight=5), B(weight=3), C(weight=2)]

第 1 次 Pick

进入 Pick()，加锁后跳过 equalNodes 检查（首次调用，lastNodes 为空会触发初始化，但不影响权重逻辑，此处省略节点变更处理细节，聚焦核心循环）。

var totalWeight float64          // totalWeight = 0
var selected selector.WeightedNode // selected = nil
var selectWeight float64         // selectWeight = 0

遍历 node = A (weight=5)
totalWeight += node.Weight()     // totalWeight = 0 + 5 = 5

cwt := p.currentWeight["A"]      // map 中无 "A"，Go zero value → cwt = 0
cwt += node.Weight()             // cwt = 0 + 5 = 5
p.currentWeight["A"] = cwt       // currentWeight = {A:5}

// selected == nil → 条件成立
selectWeight = cwt               // selectWeight = 5
selected = node                  // selected = A

遍历 node = B (weight=3)
totalWeight += node.Weight()     // totalWeight = 5 + 3 = 8

cwt := p.currentWeight["B"]      // cwt = 0
cwt += node.Weight()             // cwt = 0 + 3 = 3
p.currentWeight["B"] = cwt       // currentWeight = {A:5, B:3}

// selectWeight(5) < cwt(3)? → false，不更新

遍历 node = C (weight=2)
totalWeight += node.Weight()     // totalWeight = 8 + 2 = 10

cwt := p.currentWeight["C"]      // cwt = 0
cwt += node.Weight()             // cwt = 0 + 2 = 2
p.currentWeight["C"] = cwt       // currentWeight = {A:5, B:3, C:2}

// selectWeight(5) < cwt(2)? → false，不更新

循环结束，扣减总权重
// selected = A, selectWeight = 5, totalWeight = 10
p.currentWeight["A"] = selectWeight - totalWeight
// currentWeight["A"] = 5 - 10 = -5
// ✅ currentWeight = {A:-5, B:3, C:2}

🎯 第 1 次结果：选中 A

第 2 次 Pick

var totalWeight float64          // 重置为 0
var selected selector.WeightedNode // 重置为 nil
var selectWeight float64         // 重置为 0

遍历 node = A (weight=5)
totalWeight += 5                 // totalWeight = 5

cwt := p.currentWeight["A"]      // cwt = -5 ← 上轮遗留！
cwt += node.Weight()             // cwt = -5 + 5 = 0
p.currentWeight["A"] = 0         // currentWeight = {A:0, B:3, C:2}

// selected == nil → 条件成立
selectWeight = 0                 // selectWeight = 0
selected = A                     // selected = A（暂时）

遍历 node = B (weight=3)
totalWeight += 3                 // totalWeight = 8

cwt := p.currentWeight["B"]      // cwt = 3
cwt += node.Weight()             // cwt = 3 + 3 = 6
p.currentWeight["B"] = 6         // currentWeight = {A:0, B:6, C:2}

// selectWeight(0) < cwt(6)? → true ✅ 更新！
selectWeight = 6                 // selectWeight = 6
selected = B                     // selected = B

⚠️ 注意这里：虽然 A 先遍历并被暂时选中，但 B 的累积权重 6 > A 的 0，B 覆盖了 A。这就是平滑的核心——上轮被选中的 A 因为扣了 totalWeight，本轮累积值变低，把机会让给了 B。

遍历 node = C (weight=2)
totalWeight += 2                 // totalWeight = 10

cwt := p.currentWeight["C"]      // cwt = 2
cwt += node.Weight()             // cwt = 2 + 2 = 4
p.currentWeight["C"] = 4         // currentWeight = {A:0, B:6, C:4}

// selectWeight(6) < cwt(4)? → false，不更新

扣减
p.currentWeight["B"] = 6 - 10    // = -4
// ✅ currentWeight = {A:0, B:-4, C:4}

🎯 第 2 次结果：选中 B

第 3 次 Pick

直接给出每步关键值：
节点   上轮 cwt   +weight   累加后 cwt   是否更新 selected
A   0   +5   5   ✅ (nil→A, selectWeight=5)

B   -4   +3   -1   ❌ (5 > -1)

C   4   +2   6   ✅ (6 > 5, selected→C)

// 扣减：currentWeight["C"] = 6 - 10 = -4
// ✅ currentWeight = {A:5, B:-1, C:-4}

🎯 第 3 次结果：选中 C

完整 10 轮推演表
轮次   A 累加后   B 累加后   C 累加后   选中   扣减后 A   扣减后 B   扣减后 C
1   5   3   2   A   -5   3   2

2   0   6   4   B   0   -4   4

3   5   -1   6   C   5   -1   -4

4   10   2   -2   A   0   2   -2

5   5   5   0   A   -5   5   0

6   0   8   2   B   0   -2   2

7   5   1   4   A   -5   1   4

8   0   4   6   C   0   4   -4

9   5   7   -2   B   5   -3   -2

10   10   0   0   A   0   0   0

统计：A=5次, B=3次, C=2次 → 精确 5:3:2 ✅

调度序列：A → B → C → A → A → B → A → C → B → A

对比普通 WRR 的 AAAAABBBCC，可以看到请求被均匀打散，没有任何节点连续承受超过 2 次请求。

🔑 回到你的问题：顺序依赖在哪里体现？

看 第 5 轮：
节点   累加后 cwt
A   5

B   5

A 和 B 的 currentWeight 完全相等。代码中：
if selected == nil || selectWeight < cwt {

条件是 严格小于 <，不是 <=。所以 B 的 5 不会覆盖 A 的 5，A 因为排在切片前面而胜出。

如果节点顺序是 [B, A, C]，第 5 轮就会选 B 而非 A。

但请注意：无论谁在第 5 轮胜出，10 轮结束后 A 仍然恰好被选 5 次、B 恰好 3 次。顺序只改变了序列的排列方式，没有改变比例。而且第 10 轮结束时所有 currentWeight 归零，下一个周期重新开始，偏差不会累积。
*/

const (
	// Name is wrr(Weighted Round Robin) balancer name
	Name = "wrr"
)

var _ selector.Balancer = (*Balancer)(nil)

// Option is wrr builder option.
type Option func(o *options)

// options is wrr builder options
type options struct{}

// Balancer is a wrr balancer.
type Balancer struct {
	mu            sync.Mutex
	currentWeight map[string]float64
	lastNodes     []selector.WeightedNode
}

// equalNodes checks if two slices of WeightedNode contain the same nodes
func equalNodes(a, b []selector.WeightedNode) bool {
	if len(a) != len(b) {
		return false
	}

	// Create a map of addresses from slice a
	aMap := make(map[string]bool, len(a))
	for _, node := range a {
		aMap[node.Address()] = true
	}

	// Check if all nodes in slice b exist in slice a
	for _, node := range b {
		if !aMap[node.Address()] {
			return false
		}
	}

	return true
}

// New random a selector.
func New(opts ...Option) selector.Selector {
	return NewBuilder(opts...).Build()
}

// Pick is pick a weighted node.
func (p *Balancer) Pick(_ context.Context, nodes []selector.WeightedNode) (selector.WeightedNode, selector.DoneFunc, error) {
	if len(nodes) == 0 {
		return nil, nil, selector.ErrNoAvailable
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if the node list has changed
	if !equalNodes(p.lastNodes, nodes) {
		// Update lastNodes
		p.lastNodes = make([]selector.WeightedNode, len(nodes))
		copy(p.lastNodes, nodes)

		// Create a set of current node addresses for cleanup
		currentNodes := make(map[string]bool, len(nodes))
		for _, node := range nodes {
			currentNodes[node.Address()] = true
		}

		// Clean up stale entries from currentWeight map
		for address := range p.currentWeight {
			if !currentNodes[address] {
				delete(p.currentWeight, address)
			}
		}
	}

	var totalWeight float64
	var selected selector.WeightedNode
	var selectWeight float64

	// nginx wrr load balancing algorithm: http://blog.csdn.net/zhangskd/article/details/50194069
	for _, node := range nodes {
		totalWeight += node.Weight()
		cwt := p.currentWeight[node.Address()]
		// current += effectiveWeight
		cwt += node.Weight()
		p.currentWeight[node.Address()] = cwt
		if selected == nil || selectWeight < cwt {
			selectWeight = cwt
			selected = node
		}
	}
	p.currentWeight[selected.Address()] = selectWeight - totalWeight

	d := selected.Pick()
	return selected, d, nil
}

// NewBuilder returns a selector builder with wrr balancer
func NewBuilder(opts ...Option) selector.Builder {
	var option options
	for _, opt := range opts {
		opt(&option)
	}
	return &selector.DefaultBuilder{
		Balancer: &Builder{},
		Node:     &direct.Builder{},
	}
}

// Builder is wrr builder
type Builder struct{}

// Build creates Balancer
func (b *Builder) Build() selector.Balancer {
	return &Balancer{currentWeight: make(map[string]float64)}
}
