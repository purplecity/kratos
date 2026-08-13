package ewma

import (
	"context"
	"math"
	"net"
	"sync/atomic"
	"time"

	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/selector"
)

/*
这个文件是 Kratos 中 P2C（Power of Two Choices）负载均衡算法的核心指标采集与权重计算模块。EWMA 是 Exponentially Weighted Moving Average（指数加权移动平均） 的缩写，它是这套算法的数学基石。

一、核心设计理念

P2C 算法的本质是：随机挑两个节点，选负载更轻的那个。

要判断“谁更轻”，需要一个综合指标，这个指标必须回答三个问题：

响应快不快？ → lag（延迟）
当前忙不忙？ → inflight（在途请求数）
可不可靠？ → success（成功率）

EWMA Node 的全部工作就是：实时采集这三个指标，并合成一个 Weight 值供 Selector 使用。

二、数据结构逐字段解析

type Node struct {
    selector.Node                  // 嵌入基础 Node（地址、元数据等）

    // ─── 三大核心指标 ───
    lag       atomic.Int64         // EWMA 平滑后的平均延迟（纳秒）
    success   atomic.Uint64        // EWMA 平滑后的成功率（0~1000，‰精度）
    inflight  atomic.Int64         // 当前在途请求数（已发出但未返回）

    // ─── 在途请求的精细追踪 ───
    inflights [200]atomic.Int64    // 固定大小的环形槽，记录每个在途请求的开始时间
    reqs      atomic.Int64         // 累计请求数，用于环形槽取模

    // ─── 时间戳 ───
    stamp     atomic.Int64         // 上一次完成请求的时间戳（用于计算衰减系数 w）
    lastPick  atomic.Int64         // 上一次被 Pick 的时间戳

    // ─── 辅助 ───
    errHandler   func(err error) (isErr bool)  // 自定义错误判断（区分业务错误 vs 传输错误）
    cachedWeight *atomic.Value                 // 缓存计算出的权重，5ms 内不重复计算
}

为什么用 atomic 而不是 sync.Mutex？

因为 Pick 和 Done 回调可能在不同的 goroutine 中被高并发调用。用 atomic 实现无锁更新，避免锁竞争成为性能瓶颈。

三、EWMA 衰减机制（核心数学原理）

const tau = int64(time.Millisecond * 600)

tau 是时间常数，决定了历史数据的衰减速度。衰减系数 w 的计算：

w = e^{-Delta t / tau}
Δt（两次请求间隔）   w 值   含义
0ms   1.0   完全信任历史值，新数据权重为 0

600ms   ~0.37   历史值保留 37%，新数据占 63%

1200ms   ~0.14   历史值保留 14%，新数据占 86%

5000ms   ~0.0002   几乎完全信任新数据

物理意义：如果节点很久没收到请求，历史指标会快速衰减，新的一次请求将主导指标。这避免了“很久以前的慢请求一直拖累节点权重”的问题。

四、关键方法逐段解析

Pick() — 请求开始时的钩子

func (n *Node) Pick() selector.DoneFunc {
    start := time.Now().UnixNano()
    n.lastPick.Store(start)        // 记录被选中的时间
    n.inflight.Add(1)              // 在途请求数 +1
    reqs := n.reqs.Add(1)
    slot := reqs % 200             // 环形槽定位
    swapped := n.inflights[slot].CompareAndSwap(0, start)  // 记录开始时间
    return func(_ context.Context, di selector.DoneInfo) {
        // ... Done 回调
    }
}

环形槽 inflights[200] 的设计意图：

这个数组不是存储所有请求，而是一个固定大小的滑动窗口。每个槽位存储的是“第 N 个请求开始时间”。当槽位非 0 时，说明对应请求尚未返回（仍在途）。这个数据是给 predict() 用的。

⚠️ 注意 swapped 变量
如果 CompareAndSwap 失败（槽位已被占用），说明发生了哈希冲突（第 N 个和第 N-200 个请求同时在途）。此时 swapped=false，Done 时就不会去清理这个槽位，避免误删别人的数据。这是一个巧妙的无锁近似设计——在 200 个槽位下冲突概率极低。

Done 回调 — 请求完成时的指标更新

这是 EWMA 的核心写入点：

return func(_ context.Context, di selector.DoneInfo) {
    // ① 清理环形槽
    if swapped {
        n.inflights[slot].CompareAndSwap(start, 0)
    }
    n.inflight.Add(-1)

    // ② 计算衰减系数 w
    now := time.Now().UnixNano()
    stamp := n.stamp.Swap(now)
    td := now - stamp
    w := math.Exp(float64(-td) / float64(tau))

    // ③ EWMA 更新 lag
    lag := now - start
    oldLag := n.lag.Load()
    if oldLag == 0 {
        w = 0.0  // 第一次请求，完全信任新值
    }
    lag = int64(float64(oldLagw + float64(lag)(1.0-w))
    n.lag.Store(lag)

    // ④ EWMA 更新 success
    success := uint64(1000)  // 默认成功，值 1000‰
    if di.Err != nil {
        // 判断是否是“真正的失败”（区分业务错误 vs 网络错误）
        if n.errHandler != nil && n.errHandler(di.Err) {
            success = 0
        }
        // 标准网络/超时错误一律视为失败
        if errors.Is(context.DeadlineExceeded, di.Err) || ... {
            success = 0
        }
    }
    oldSuc := n.success.Load()
    success = uint64(float64(oldSucw + float64(success)(1.0-w))
    n.success.Store(success)
}

关键细节：
设计点   解释
success 初始值 1000   新节点默认完全可信，避免冷启动时权重过低

oldLag == 0 时 w = 0   第一次请求没有历史参考，直接用新值，不做平滑

errHandler 自定义错误判断   允许用户区分“业务返回的错误码”（如参数校验失败，不应影响权重）和“真正的故障”（如超时、网络断开，应降低权重）

load() — 负载计算（最复杂的部分）

func (n *Node) load() (load uint64) {
    now := time.Now().UnixNano()
    avgLag := n.lag.Load()
    predict := n.predict(avgLag, now)

    if avgLag == 0 {
        // 没有历史数据时，用惩罚值代替
        load = penalty * uint64(n.inflight.Load())
        return
    }
    if predict > avgLag {
        avgLag = predict  // 如果预测延迟更高，用更悲观的值
    }
    // +5ms 消除跨可用区的延迟差异
    avgLag += int64(time.Millisecond * 5)
    // 开平方压缩极端值
    avgLag = int64(math.Sqrt(float64(avgLag)))
    // 最终负载 = 延迟 × 在途数
    load = uint64(avgLag) * uint64(n.inflight.Load())
    return load
}

公式解读：

text{load} = sqrt{max(text{lag}, text{predict}) + 5text{ms}} times text{inflight}
因子   作用
predict   捕捉“正在恶化但尚未反映在 EWMA 中的延迟”（见下文）

+5ms   让跨可用区节点不会被微小延迟差异过度歧视

math.Sqrt   压缩极端慢请求的影响，避免一次毛刺导致节点被长期冷落

× inflight   在途请求越多，负载越重——这是核心反馈信号

predict() — 延迟预测（前瞻性指标）

func (n *Node) predict(avgLag int64, now int64) (predict int64) {
    var total int64
    var slowNum, totalNum int
    for i := range n.inflights {
        start := n.inflights[i].Load()
        if start != 0 {
            totalNum++
            lag := now - start
            if lag > avgLag {
                slowNum++
                total += lag
            }
        }
    }
    if slowNum >= (totalNum/2 + 1) {
        predict = total / int64(slowNum)
    }
    return
}

它解决什么问题？

EWMA 的 lag 是滞后指标——它反映的是过去完成请求的平均延迟。但如果节点突然变慢，正在途中的请求还没完成，lag 不会立即上升。

predict() 扫描所有在途请求，如果发现超过一半的请求已经比历史平均延迟还慢，就用这些慢请求的平均延迟作为预测值。这是一个前瞻性信号，让权重能在 EWMA 反应过来之前就提前下降。

⚠️ 触发条件严格：slowNum >= totalNum/2 + 1
必须超过半数在途请求都慢才触发，避免少量偶发慢请求误判为节点恶化。

Weight() — 最终权重合成

func (n *Node) Weight() (weight float64) {
    w, ok := n.cachedWeight.Load().(*nodeWeight)
    now := time.Now().UnixNano()
    if !ok || time.Duration(now-w.updateAt) > (time.Millisecond*5) {
        health := n.health()
        load := n.load()
        weight = float64(healtuint64(time.Microsecond)10) / float64(load)
        n.cachedWeight.Store(&nodeWeight{
            value:    weight,
            updateAt: now,
        })
    } else {
        weight = w.value
    }
    return
}

公式：

text{weight} = frac{text{health} times 10^4}{text{load}}

分子（health）：成功率越高，权重越大
分母（load）：延迟越高、在途越多，权重越小
5ms 缓存：权重计算涉及浮点运算和多次 atomic 读取，在高频 Pick 场景下缓存 5ms 可显著降低 CPU 开销，且 5ms 的指标变化对决策影响可忽略

五、完整生命周期示例

时间轴
  │
  ├─ t0: Selector 随机选 A 和 B 两个节点
  │       A.Weight() = 850  ← health=980, load=12
  │       B.Weight() = 320  ← health=600, load=19
  │       → 选 A
  │
  ├─ t1: A.Pick()
  │       inflight: 0 → 1
  │       inflights[42] = t1（开始时间）
  │
  ├─ t2: A 的请求完成，Done() 被调用
  │       lag = t2 - t1 = 15ms
  │       EWMA 更新: newLag = oldLaw + 15ms(1-w)
  │       success = 1000（成功）
  │       inflight: 1 → 0
  │
  ├─ t3: A 突然变慢，3个请求同时发出
  │       inflight: 0 → 3
  │       inflights[43,44,45] = t3
  │
  ├─ t4: predict() 被调用
  │       3个在途请求都已耗时 > avgLag
  │       slowNum=3 > totalNum/2+1=2
  │       predict = 平均慢延迟 → load 上升 → weight 下降
  │       → 下次 P2C 大概率不选 A
  │
  └─ t5: 慢请求陆续返回，Done() 更新 lag
          EWMA lag 上升，与 predict 收敛
          weight 稳定在较低水平

📌 总结

ewma.Node 是一个自适应性能探针，它用三种互补机制精确感知每个后端节点的实时状态：
机制   类型   作用
EWMA lag/success   滞后指标   平滑历史延迟和成功率，过滤噪声

predict()   前瞻指标   捕捉正在恶化但尚未完成请求的延迟趋势

inflight   实时指标   当前并发压力

三者合成 weight = health / load，供 P2C 算法在两个随机节点间做出最优选择。整套设计无需任何全局配置，节点权重完全由实际流量反馈驱动，是 Kratos 负载均衡体系中最精妙的部分之一。
*/

const (
	// The mean lifetime of `cost`, it reaches its half-life after Tau*ln(2).
	tau = int64(time.Millisecond * 600)
	// if statistic not collected,we add a big lag penalty to endpoint
	penalty = uint64(time.Microsecond * 100)
)

var (
	_ selector.WeightedNode        = (*Node)(nil)
	_ selector.WeightedNodeBuilder = (*Builder)(nil)
)

// Node is endpoint instance
type Node struct {
	selector.Node

	// client statistic data
	lag       atomic.Int64
	success   atomic.Uint64
	inflight  atomic.Int64
	inflights [200]atomic.Int64
	// last collected timestamp
	stamp atomic.Int64
	// request number in a period time
	reqs atomic.Int64
	// last lastPick timestamp
	lastPick atomic.Int64

	errHandler   func(err error) (isErr bool)
	cachedWeight *atomic.Value
}

type nodeWeight struct {
	value    float64
	updateAt int64
}

// Builder is ewma node builder.
type Builder struct {
	ErrHandler func(err error) (isErr bool)
}

// Build create a weighted node.
func (b *Builder) Build(n selector.Node) selector.WeightedNode {
	s := &Node{
		Node:         n,
		inflights:    [200]atomic.Int64{},
		errHandler:   b.ErrHandler,
		cachedWeight: &atomic.Value{},
	}
	s.success.Store(1000)
	s.inflight.Store(1)
	return s
}

func (n *Node) health() uint64 {
	return n.success.Load()
}

func (n *Node) load() (load uint64) {
	now := time.Now().UnixNano()
	avgLag := n.lag.Load()
	predict := n.predict(avgLag, now)

	if avgLag == 0 {
		// penalty is the penalty value when there is no data when the node is just started.
		load = penalty * uint64(n.inflight.Load())
		return
	}
	if predict > avgLag {
		avgLag = predict
	}
	// add 5ms to eliminate the latency gap between different zones
	avgLag += int64(time.Millisecond * 5)
	avgLag = int64(math.Sqrt(float64(avgLag)))
	load = uint64(avgLag) * uint64(n.inflight.Load())
	return load
}

func (n *Node) predict(avgLag int64, now int64) (predict int64) {
	var (
		total    int64
		slowNum  int
		totalNum int
	)
	for i := range n.inflights {
		start := n.inflights[i].Load()
		if start != 0 {
			totalNum++
			lag := now - start
			if lag > avgLag {
				slowNum++
				total += lag
			}
		}
	}
	if slowNum >= (totalNum/2 + 1) {
		predict = total / int64(slowNum)
	}
	return
}

// Pick pick a node.
func (n *Node) Pick() selector.DoneFunc {
	start := time.Now().UnixNano()
	n.lastPick.Store(start)
	n.inflight.Add(1)
	reqs := n.reqs.Add(1)
	slot := reqs % 200
	swapped := n.inflights[slot].CompareAndSwap(0, start)
	return func(_ context.Context, di selector.DoneInfo) {
		if swapped {
			n.inflights[slot].CompareAndSwap(start, 0)
		}
		n.inflight.Add(-1)

		now := time.Now().UnixNano()
		// get moving average ratio w
		stamp := n.stamp.Swap(now)
		td := now - stamp
		if td < 0 {
			td = 0
		}
		w := math.Exp(float64(-td) / float64(tau))

		lag := now - start
		if lag < 0 {
			lag = 0
		}
		oldLag := n.lag.Load()
		if oldLag == 0 {
			w = 0.0
		}
		lag = int64(float64(oldLag)*w + float64(lag)*(1.0-w))
		n.lag.Store(lag)

		success := uint64(1000) // error value ,if error set 1
		if di.Err != nil {
			if n.errHandler != nil && n.errHandler(di.Err) {
				success = 0
			}
			var netErr net.Error
			if errors.Is(context.DeadlineExceeded, di.Err) || errors.Is(context.Canceled, di.Err) ||
				errors.IsServiceUnavailable(di.Err) || errors.IsGatewayTimeout(di.Err) || errors.As(di.Err, &netErr) {
				success = 0
			}
		}
		oldSuc := n.success.Load()
		success = uint64(float64(oldSuc)*w + float64(success)*(1.0-w))
		n.success.Store(success)
	}
}

// Weight is node effective weight.
func (n *Node) Weight() (weight float64) {
	w, ok := n.cachedWeight.Load().(*nodeWeight)
	now := time.Now().UnixNano()
	if !ok || time.Duration(now-w.updateAt) > (time.Millisecond*5) {
		health := n.health()
		load := n.load()
		weight = float64(health*uint64(time.Microsecond)*10) / float64(load)
		n.cachedWeight.Store(&nodeWeight{
			value:    weight,
			updateAt: now,
		})
	} else {
		weight = w.value
	}
	return
}

func (n *Node) PickElapsed() time.Duration {
	return time.Duration(time.Now().UnixNano() - n.lastPick.Load())
}

func (n *Node) Raw() selector.Node {
	return n.Node
}
