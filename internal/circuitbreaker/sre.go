package circuitbreaker

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

/*
这段代码是 Kratos 框架中最核心、最精妙的组件之一。它不是传统的 Hystrix 状态机熔断器（Closed/Open/Half-Open），而是基于 Google SRE 书中提到的自适应概率降级（Adaptive Drop） 理念实现的。

为了让你彻底看懂，我们从核心思想、数学公式、代码拆解到场景推演，一步步来。

一、 核心思想：SRE 概率熔断 vs 传统状态机熔断
特性   传统 Hystrix 熔断器   Kratos SRE 熔断器 (本代码)
触发机制   失败率达到阈值 → 100% 拒绝所有请求   失败率越高 → 按概率丢弃请求（如丢弃 30%）

恢复机制   等待超时 → 放行 1 个探测请求 (Half-Open)   失败率下降 → 丢弃概率自动平滑降低

流量曲线   悬崖式（要么全放，要么全断）   平滑曲线（渐进式降级与恢复）

致命缺陷   "试探风暴"：恢复瞬间大量请求涌入再次打垮下游   无此问题，天然平滑

💡 一句话理解
传统熔断器像电闸，跳闸后全屋断电，合闸后瞬间全亮；
SRE 熔断器像水龙头，水质变差（失败率高）时，你把它关小一点（丢弃部分请求），水质好转时，你再慢慢拧大。

二、 核心数学公式（灵魂所在）

Google SRE 书中给出的丢弃概率公式是：

	text{Drop Probability} = maxleft(0, frac{text{requests} - K times text{accepts}}{text{requests} + 1}right)

对应到 Kratos 代码中的变量：
requests = total（总请求数）
accepts = successes（成功请求数）
K = k（敏感度乘数）

代码中的实现：
// k = 1 / (1 - failureRatio)
requests := b.k * float64(successes)  // 即 K * accepts
dropRatio := math.Max(0, (float64(total)-requests)/float64(total+1))

K 值（k）的物理意义
假设你设置的 failureRatio（最大容忍失败率）是 50% (0.5)：

	K = frac{1}{1 - 0.5} = 2

这意味着：只要总请求数 total 超过了成功数 successes 的 2 倍（即失败率超过 50%），分子就会大于 0，开始产生丢弃概率。

如果 failureRatio 设为 10% (0.1)：

	K = frac{1}{1 - 0.1} approx 1.11

此时容忍度极低，稍微有一点失败，分子就会大于 0，迅速开始丢弃请求。

三、 代码逐块深度拆解

滑动窗口计数器 (rollingCounter)
这是统计成功数和总请求数的基础设施，采用了环形数组（Ring Buffer） 思想。

// 假设 window=3s, bucket=10 → bucketDuration = 300ms

	func (r *rollingCounter) add(success int64) {
	    slot := r.currentSlot() // 当前时间 / 300ms，得到一个单调递增的整数槽位号
	    offset := int(slot % int64(len(r.buckets))) // 取模，映射到 0~9 的环形数组索引

	    r.mu.Lock()
	    defer r.mu.Unlock()
	    bucket := &r.buckets[offset]

	    // 如果这个桶记录的 slot 不是当前 slot，说明是上一圈的旧数据，直接清零重置
	    if bucket.slot != slot {
	        bucket.slot = slot
	        bucket.success = 0
	        bucket.total = 0
	    }
	    bucket.success += success // 累加成功数 (0 或 1)
	    bucket.total++            // 累加总请求数
	}

为什么用环形数组？
内存固定（永远只有 10 个桶），不需要频繁分配/回收内存。
自动淘汰过期数据：当 slot 增加时，旧桶会被自然覆盖清零。
summary() 遍历这 10 个桶时，通过 slot - bucket.slot >= size 过滤掉时间上已经过期的桶，只累加最近 3s 内的有效数据。

请求准入判断 (Allow) —— 最核心的逻辑

	func (b *Breaker) Allow() error {
	    successes, total := b.stat.summary() // 获取最近 3s 内的成功数和总数

	    // 计算 "期望的最大请求数" = K * 成功数
	    requests := b.k * float64(successes)

	    // 【冷启动保护】如果总请求数太少（<20），样本不足，直接放行
	    // 【健康状态】如果 total < requests (即 total < K * successes)，说明失败率还没到阈值，放行
	    if total < b.request || float64(total) < requests {
	        atomic.CompareAndSwapInt32(&b.state, StateOpen, StateClosed) // 仅用于监控打点
	        return nil // ✅ 放行
	    }

	    // 走到这里，说明失败率已经超标，进入 "Open" 状态（仅用于监控打点）
	    atomic.CompareAndSwapInt32(&b.state, StateClosed, StateOpen)

	    // 计算丢弃概率
	    // 分子：total - requests (超标的请求数)
	    // 分母：total + 1 (加 1 是为了防止除零，同时让概率永远不会绝对等于 100%)
	    dropRatio := math.Max(0, (float64(total)-requests)/float64(total+1))

	    // 掷骰子：如果随机数 < 丢弃概率，则拒绝请求
	    if b.random() < dropRatio {
	        return ErrNotAllowed // ❌ 拒绝
	    }
	    return nil // ✅ 放行
	}

⚠️ 关键细节：分母为什么是 total + 1？
如果分母是 total，当 successes = 0（全失败）时，requests = 0，dropRatio = total / total = 1.0（100% 拒绝）。
这会导致下游彻底失去被探测的机会（和传统熔断器的 Open 状态一样）。
分母加 1 后，dropRatio = total / (total + 1)，永远小于 1.0（例如 99/100 = 99%）。这意味着即使下游全挂，也总有 1% 的"漏网之鱼"能穿透熔断器去探测下游是否恢复。这就是 SRE 熔断器不需要 Half-Open 状态的原因！

结果反馈 (MarkSuccess / MarkFailed)

func (b *Breaker) MarkSuccess() { b.stat.add(1) } // 成功：success+1, total+1
func (b *Breaker) MarkFailed()  { b.stat.add(0) } // 失败：success+0, total+1

极其简洁，只是往滑动窗口里写数据。

四、 场景推演：用数字感受"平滑丢弃"

假设配置：failureRatio = 0.5 (则 K=2)，request = 20 (最小样本数 20)。
场景   total (总请求)   successes (成功)   K times successes   失败率   dropRatio 计算   丢弃概率   行为
刚启动   10   8   16   20%   total(10) < request(20)   0%   样本不足，全放

完全健康   100   90   180   10%   total(100) < 180   0%   低于阈值，全放

临界点   100   50   100   50%   total(100) == 100   0%   刚好达到阈值，不丢

开始恶化   100   40   80   60%   (100 - 80) / 101   ~19.8%   开始丢弃约 20% 的请求

严重恶化   100   20   40   80%   (100 - 40) / 101   ~59.4%   丢弃近 60% 的请求

彻底宕机   100   0   0   100%   (100 - 0) / 101   ~99.0%   丢弃 99%，仍留 1% 探测

观察这个曲线：
失败率 50% 之前：0% 丢弃（完全放行）
失败率 60%：丢弃 20%
失败率 80%：丢弃 60%
失败率 100%：丢弃 99%

这就是自适应的魅力：下游稍微有点抖动，我只丢一点点请求帮你减压；下游快挂了，我帮你挡住绝大部分流量；下游只要有一丝好转（成功率从 0 变成 1），K * successes 就会变大，dropRatio 就会立刻下降，流量自动平滑恢复。

五、 代码中的几个精妙设计总结

state 字段是个"假"状态

	你会发现 StateOpen 和 StateClosed 只用了 atomic.CompareAndSwapInt32 来更新，但在 Allow() 的核心判断逻辑中，完全没有读取这个 state！
	这个 state 仅仅是暴露给外部监控系统（如 Prometheus metrics）做打点用的。真正的熔断判断，完全由实时的数学公式（dropRatio）驱动。

random 函数的线程安全

	   random: func() float64 { randMu.Lock(); defer randMu.Unlock(); return rnd.Float64() }

	Go 1.20 之前的 math/rand 全局函数不是并发安全的（Go 1.20+ 的 math/rand/v2 是安全的）。这里手动加锁包装了一个 rand.Rand 实例，保证了高并发下生成随机数不会 panic 或产生重复序列。

冷启动保护 (total < b.request)

	如果没有这个判断，服务刚启动时 total=1, successes=0，dropRatio 会瞬间飙升到 50%，导致刚启动就丢弃一半请求。request=20 保证了至少有 20 个样本后才开始计算概率。

📌 一句话总结

这段代码实现了一个没有状态机、不依赖定时器、纯靠实时数学公式驱动的自适应熔断器。它通过环形数组统计滑动窗口指标，利用  max(0, frac{total - K times success}{total + 1})  公式计算出平滑的丢弃概率，完美解决了传统熔断器"悬崖式跳闸"和"恢复期试探风暴"的痛点，是微服务流量治理的顶级工程实践。
*/
const (
	// StateOpen rejects requests according to the calculated drop ratio.
	StateOpen int32 = iota
	// StateClosed allows requests while the rolling failure ratio is healthy.
	StateClosed
)

// Option configures the SRE circuit breaker.
type Option func(*options)

type options struct {
	failureRatio float64
	request      int64
	bucket       int
	window       time.Duration
}

// WithFailureRatio sets the failure ratio threshold that starts rejection.
func WithFailureRatio(ratio float64) Option {
	return func(o *options) {
		o.failureRatio = ratio
	}
}

// WithRequest sets the minimum request count before rejection starts.
func WithRequest(r int64) Option {
	return func(o *options) {
		o.request = r
	}
}

// WithWindow sets the rolling statistical window.
func WithWindow(d time.Duration) Option {
	return func(o *options) {
		o.window = d
	}
}

// WithBucket sets the number of buckets in the rolling window.
func WithBucket(b int) Option {
	return func(o *options) {
		o.bucket = b
	}
}

// Breaker is an SRE-style circuit breaker.
type Breaker struct {
	stat *rollingCounter

	random func() float64

	k       float64
	request int64
	state   int32
}

// NewBreaker returns an SRE circuit breaker.
func NewBreaker(opts ...Option) CircuitBreaker {
	opt := options{
		failureRatio: 0.5,
		request:      20,
		bucket:       10,
		window:       3 * time.Second,
	}
	for _, o := range opts {
		o(&opt)
	}
	if opt.failureRatio < 0 || opt.failureRatio >= 1 {
		opt.failureRatio = 0.5
	}
	if opt.request < 1 {
		opt.request = 1
	}
	if opt.bucket < 1 {
		opt.bucket = 1
	}
	if opt.window <= 0 {
		opt.window = 3 * time.Second
	}
	bucketDuration := opt.window / time.Duration(opt.bucket)
	if bucketDuration <= 0 {
		bucketDuration = opt.window
	}
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	var randMu sync.Mutex
	return &Breaker{
		stat:    newRollingCounter(opt.bucket, bucketDuration),
		random:  func() float64 { randMu.Lock(); defer randMu.Unlock(); return rnd.Float64() },
		request: opt.request,
		k:       1 / (1 - opt.failureRatio),
		state:   StateClosed,
	}
}

// Allow reports whether the request can pass the breaker.
func (b *Breaker) Allow() error {
	successes, total := b.stat.summary()
	requests := b.k * float64(successes)
	if total < b.request || float64(total) < requests {
		atomic.CompareAndSwapInt32(&b.state, StateOpen, StateClosed)
		return nil
	}
	atomic.CompareAndSwapInt32(&b.state, StateClosed, StateOpen)
	dropRatio := math.Max(0, (float64(total)-requests)/float64(total+1))
	if b.random() < dropRatio {
		return ErrNotAllowed
	}
	return nil
}

// MarkSuccess records a successful request.
func (b *Breaker) MarkSuccess() {
	b.stat.add(1)
}

// MarkFailed records a failed request.
func (b *Breaker) MarkFailed() {
	b.stat.add(0)
}

type rollingCounter struct {
	mu             sync.Mutex
	buckets        []counterBucket
	bucketDuration time.Duration
}

type counterBucket struct {
	slot    int64
	success int64
	total   int64
}

func newRollingCounter(size int, bucketDuration time.Duration) *rollingCounter {
	return &rollingCounter{
		buckets:        make([]counterBucket, size),
		bucketDuration: bucketDuration,
	}
}

func (r *rollingCounter) add(success int64) {
	slot := r.currentSlot()
	offset := int(slot % int64(len(r.buckets)))

	r.mu.Lock()
	defer r.mu.Unlock()
	bucket := &r.buckets[offset]
	if bucket.slot != slot {
		bucket.slot = slot
		bucket.success = 0
		bucket.total = 0
	}
	bucket.success += success
	bucket.total++
}

/*
summary() 方法是整个 SRE 熔断器的数据基石。它的职责是从环形数组（Ring Buffer）中提取出当前有效滑动窗口内的 successes（成功数）和 total（总请求数）。

这段代码虽然短，但包含了三个极易踩坑的工程细节。我们逐行拆解：

完整代码与核心逻辑

func (r *rollingCounter) summary() (success int64, total int64) {
    slot := r.currentSlot()      // ① 获取当前时间对应的槽位号
    size := int64(len(r.buckets)) // ② 获取环形数组的长度（桶数量）

    r.mu.Lock()                  // ③ 加锁，保证读取过程中数据不被 add() 修改
    defer r.mu.Unlock()

    for _, bucket := range r.buckets {
        // ④ 核心过滤条件：跳过无效桶
        if bucket.total == 0 || slot-bucket.slot >= size || bucket.slot > slot {
            continue
        }
        success += bucket.success // ⑤ 累加有效桶的成功数
        total += bucket.total     // ⑥ 累加有效桶的总请求数
    }
    return success, total
}

它的本质就是一个带过滤条件的遍历累加。关键在于第 ④ 步的三个过滤条件，它们共同保证了统计结果的时间准确性和数据安全性。

三个过滤条件的深度解析

这是 summary() 最核心的部分，缺一不可：

条件一：bucket.total == 0
含义：这个桶从来没有被写入过数据（或者是被重置后还没写入）。
作用：快速跳过空桶，避免无意义的累加。这是一个性能优化。

条件二：slot - bucket.slot >= size （⏰ 过期淘汰）
含义：当前槽位号减去桶记录的槽位号，差值大于等于桶的数量。说明这个桶的数据已经超出了滑动窗口的时间范围。
原理推演：
    假设 window=3s, bucket=10 → bucketDuration=300ms，size=10。
    当前 slot = 100，有效窗口是 [91, 100]（最近 10 个槽位）
    某个桶 bucket.slot = 90 → 100 - 90 = 10 >= 10 → 过期，跳过 ✅
    某个桶 bucket.slot = 91 → 100 - 91 = 9 < 10 → 有效，累加 ✅
作用：这就是"滑动窗口"的"滑动"二字的具体实现。不需要删除旧数据，只需要在读取时忽略它。旧数据会在下次 add() 时被自然覆盖重置。

条件三：bucket.slot > slot （🛡️ 防御未来数据）
含义：桶记录的槽位号比当前槽位号还大。这在正常时序下不可能发生。
为什么需要这个防御？
    系统时钟回拨：NTP 同步导致 time.Now() 突然变小，currentSlot() 返回了一个比之前更小的值。此时旧的桶里记录着"未来"的 slot，如果不跳过，slot - bucket.slot 会变成负数（int64 下溢变成一个巨大的正数），导致本应有效的桶被误判为过期，或者本应过期的桶被误判为有效。
    并发竞态的极端边界：虽然加了锁，但在某些极端 CPU 调度下，add() 和 summary() 之间可能存在微秒级的时序交错。
作用：防止整数溢出导致的统计灾难。这是一个生产级代码必备的防御性编程。

💡 三个条件的协作关系
条件一：过滤空数据（性能）
条件二：过滤过去太久的数据（正确性 - 滑动窗口）
条件三：过滤未来的异常数据（安全性 - 时钟回拨防御）
三者取并集（||），只有同时不满足这三个条件的桶，才是"当前滑动窗口内的有效数据"。

为什么必须加锁？

r.mu.Lock()
defer r.mu.Unlock()

summary() 需要遍历所有桶并累加，这个过程不是原子的。如果在遍历过程中，另一个 goroutine 正在执行 add() 修改了某个桶的 success 和 total，就会出现：

读到半更新的数据：total 已经 +1 了，但 success 还没更新，导致失败率瞬间虚高。
读到不一致的 slot：slot 已经被重置为新值，但 success/total 还是旧值，导致旧数据被当作新数据累加。

加锁保证了 summary() 读到的是一个一致性快照（Consistent Snapshot）。

⚠️ 性能影响评估
这个锁的竞争程度很低：
summary() 只在 Allow() 中被调用，即每次请求一次。
遍历 10 个桶 + 几次整数加法，耗时在百纳秒级。
相比一次网络 RPC 调用（毫秒级），这个锁的开销完全可以忽略。
如果真要优化到极致，可以改用 atomic.Int64 + 无锁环形数组，但代码复杂度会飙升，且收益极小。当前的互斥锁方案是工程上的最优平衡点。

与 add() 的配合：完整的滑动窗口生命周期

把 add() 和 summary() 放在一起看，才能理解完整的设计：

时间线:  ──────────────────────────────────────────────►
槽位:   ... | 91 | 92 | 93 | 94 | 95 | 96 | 97 | 98 | 99 | 100 | ← 当前 slot=100
             ↑                                      ↑
          最旧有效                                最新写入

add(101):  slot=101, offset=101%10=1 → 覆盖 buckets[1]
           buckets[1].slot=92 ≠ 101 → 重置为 {slot:101, success:0, total:0}
           然后写入新数据

summary(): slot=100, size=10
           遍历 buckets[0..9]:
           buckets[1].slot=92 → 100-92=8 < 10 → ✅ 有效（还没被覆盖）
           buckets[2].slot=93 → 100-93=7 < 10 → ✅ 有效
           ...
           buckets[0].slot=100 → 100-100=0 < 10 → ✅ 有效

add(101) 之后立刻 summary():
           slot=101, size=10
           buckets[1].slot=101 → 101-101=0 < 10 → ✅ 有效（刚写入的新数据）
           buckets[2].slot=93 → 101-93=8 < 10 → ✅ 有效
           buckets[1] 旧的 slot=92 的数据已经被覆盖，自动消失

这就是环形数组的精妙之处：没有删除操作，只有覆盖操作；没有链表/切片的内存分配，只有固定大小的数组索引。 summary() 通过数学计算（slot - bucket.slot >= size）在逻辑上实现了"滑动"，而物理内存始终是那块固定的数组。

📌 总结

summary() 不是一个简单的求和函数，它是一个带时间维度过滤的一致性快照读取器。三个过滤条件分别解决了空桶跳过、过期淘汰、时钟回拨防御三个问题，配合互斥锁保证了并发安全，配合环形数组实现了零内存分配的滑动窗口统计。它是整个 SRE 概率熔断器能够实时、准确、低开销地感知服务健康状态的基础。
*/

func (r *rollingCounter) summary() (success int64, total int64) {
	slot := r.currentSlot()
	size := int64(len(r.buckets))

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, bucket := range r.buckets {
		if bucket.total == 0 || slot-bucket.slot >= size || bucket.slot > slot {
			continue
		}
		success += bucket.success
		total += bucket.total
	}
	return success, total
}

func (r *rollingCounter) currentSlot() int64 {
	return time.Now().UnixNano() / int64(r.bucketDuration)
}
