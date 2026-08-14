package ratelimit

import (
	"math"
	"runtime"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"
)

/*
这是一个基于 BBR (Bottleneck Bandwidth and Round-trip propagation time) 算法的自适应限流器实现，源自 Kratos 框架。与传统的固定阈值限流（如令牌桶、漏桶）不同，BBR 根据系统实时负载（CPU、响应时间、吞吐量）动态调整限流策略，核心思想是：当系统未达瓶颈时放行，接近瓶颈时按 Little's Law 计算最大并发数并拒绝超额请求。

下面按模块逐行深度解析：

全局 CPU 采样

1.1 全局变量与衰减因子
var (
    gCPU  int64   // 全局平滑后的 CPU 使用率，范围 [0, 1000]，代表 0%~100%
    decay = 0.95  // EMA（指数移动平均）衰减因子
)

gCPU 使用整数而非浮点数，避免原子操作中的浮点精度问题和性能开销。1000 表示 100%，800 表示 80%。
decay=0.95 意味着每次采样保留 95% 的历史值 + 5% 的新值，约 20 次采样（10秒）后历史权重降至 ~36%，平衡了稳定性和灵敏度。

1.2 init 启动后台协程
func init() {
    go cpuproc()
}

包加载时自动启动 CPU 采样协程。注意：这意味着只要 import 了这个包，就会启动一个永久 goroutine。

1.3 cpuproc 采样循环
func cpuproc() {
    ticker := time.NewTicker(500 * time.Millisecond)
    defer ticker.Stop()
    sampler := newCPUSampler()
    for range ticker.C {
        curCPU := sampler.usage()       // 获取当前 500ms 窗口的原始 CPU 使用率
        if curCPU < 0 {                 // 异常值保护
            continue
        }
        curCPU = minInt64(curCPU, 1000) // 钳制到 [0, 1000]
        prevCPU := atomic.LoadInt64(&gCPU)
        // EMA 平滑公式：new = old * decay + current * (1 - decay)
        cpu := int64(float64(prevCPUdecay + float64(curCPU)(1.0-decay))
        atomic.StoreInt64(&gCPU, cpu)
    }
}

500ms 采样间隔：太短噪声大，太长反应慢。500ms 是工程经验值。
EMA 平滑：消除瞬时抖动。假设 CPU 突然从 20% 跳到 80%，平滑后约需 ln(0.5)/ln(0.95) ≈ 13 个周期（6.5秒）才能到达中位值，防止限流器对毛刺过度反应。
原子读写：gCPU 被多个 goroutine 并发读取（每个请求的 shouldDrop），必须用原子操作。

1.4 cpuSampler 基于 runtime/metrics
type cpuSampler struct {
    mu        sync.Mutex
    samples   []metrics.Sample
    prevUsed  float64
    prevTotal float64
    ready     bool
}

使用 Go 1.16+ 的 runtime/metrics API，比传统的 /proc/stat 或 syscall.Getrusage 更跨平台且无 CGO 开销。
prevUsed/prevTotal 保存上一次采样的累计值，用于计算增量。
ready 标志：首次采样没有历史数据，无法算增量，返回 0 并标记就绪。

func newCPUSampler() *cpuSampler {
    return &cpuSampler{
        samples: []metrics.Sample{
            {Name: "/cpu/classes/total:cpu-seconds"},  // 所有 CPU 类的总时间
            {Name: "/cpu/classes/idle:cpu-seconds"},   // 空闲时间
        },
    }
}

这两个指标是累计单调递增的秒数，不是瞬时百分比。必须做差分才能得到区间使用率。

func (s *cpuSampler) usage() int64 {
    s.mu.Lock()
    defer s.mu.Unlock()

    metrics.Read(s.samples)                    // 一次性读取两个指标，保证一致性
    total := s.samples[0].Value.Float64()
    idle := s.samples[1].Value.Float64()
    used := total - idle                       // 非空闲 = 用户态 + 系统态 + GC 等

    if !s.ready {                              // 首次采样，建立基线
        s.prevUsed = used
        s.prevTotal = total
        s.ready = true
        return 0                               // 无法计算，返回 0
    }

    usedDelta := used - s.prevUsed             // 本窗口实际使用的 CPU 秒数
    totalDelta := total - s.prevTotal          // 本窗口总的 CPU 秒数（= 核数 × 0.5s）
    s.prevUsed = used
    s.prevTotal = total

    if totalDelta <= 0 || usedDelta < 0 {      // 防御：时钟回拨或计数器重置
        return 0
    }
    return int64((usedDelta / totalDelta) * 1000)  // 归一化到 [0, 1000]
}

为什么用差分而非直接读百分比？ runtime/metrics 只提供累计值，这是设计决定。差分法天然抗重启/重置，且精度不受采样间隔影响。
锁的作用：prevUsed/prevTotal 是有状态字段，虽然当前只有一个 goroutine 调用 usage()，但加锁是防御性设计，防止未来多调用方场景下的数据竞争。
返回值语义：usedDelta / totalDelta 就是该窗口的 CPU 利用率。乘以 1000 转为千分比整数。例如 4 核机器，500ms 内 totalDelta=2.0（4×0.5），usedDelta=1.6，则返回 800（80%）。

BBR 限流器配置

2.1 Option 模式
type options struct {
    Window       time.Duration  // 滑动窗口总时长，默认 10s
    Bucket       int            // 窗口内的桶数量，默认 100
    CPUThreshold int64          // CPU 触发限流的阈值，默认 800（80%）
    CPUQuota     float64        // 容器 CPU 配额，用于修正 GOMAXPROCS 不准的情况
}

Window/Bucket：10s 窗口分 100 个桶 → 每桶 100ms。这决定了统计的时间分辨率。
CPUThreshold=800：BBR 的核心参数。低于此值不限流，高于此值开始检查并发数。80% 是 TCP BBR 论文推荐的拥塞避免起点。
CPUQuota：在 K8s 中 GOMAXPROCS 可能等于节点核数而非 Pod limit。设置 CPUQuota=2.0 可将采集到的 CPU 使用率按比例放大，使限流器感知真实的容器负载。

2.2 NewLimiter 构造
func NewLimiter(opts ...Option) *BBR {
    opt := options{
        Window:       10 * time.Second,
        Bucket:       100,
        CPUThreshold: 800,
    }
    for _, o := range opts {
        o(&opt)
    }
    // 参数校验与兜底
    if opt.Window <= 0 { opt.Window = 10 * time.Second }
    if opt.Bucket < 1  { opt.Bucket = 1 }

    bucketDuration := opt.Window / time.Duration(opt.Bucket)
    if bucketDuration <= 0 { bucketDuration = opt.Window }

    limiter := &BBR{
        opts:            opt,
        passStat:        newRollingCounter(opt.Bucket, bucketDuration),
        rtStat:          newRollingCounter(opt.Bucket, bucketDuration),
        bucketDuration:  bucketDuration,
        bucketPerSecond: int64(time.Second / bucketDuration),
        cpu:             func() int64 { return atomic.LoadInt64(&gCPU) },
    }
    if limiter.bucketPerSecond < 1 { limiter.bucketPerSecond = 1 }

    // CPU 配额修正
    if opt.CPUQuota != 0 {
        limiter.cpu = func() int64 {
            return int64(float64(atomic.LoadInt64(&gCPU)) * float64(runtime.GOMAXPROCS(0)) / opt.CPUQuota)
        }
    }
    return limiter
}

bucketPerSecond：每秒包含多少个桶。默认 1000ms/100ms = 10。用于将"每桶统计量"换算为"每秒速率"。
CPU 配额修正公式：真实CPU = 采集CPU × GOMAXPROCS / Quota。例如节点 16 核，Pod limit 4 核，GOMAXPROCS=16，采集到 CPU=200（20% of 16核），修正后 = 200×16/4 = 800（80% of 4核），这才是容器视角的真实负载。

核心指标计算（Little's Law）

3.1 maxPASS — 滑动窗口最大吞吐量
func (l *BBR) maxPASS() int64 {
    passCache := l.maxPASSCache.Load()
    if passCache != nil {
        ps := passCache.(*counterCache)
        if l.timespan(ps.time) < 1 {   // 缓存未过期（< 1个桶时长）
            return ps.val
        }
    }
    // reduce 遍历所有有效桶，取 count 的最大值
    rawMaxPass := int64(l.passStat.reduce(func(bucket counterBucket) float64 {
        return float64(bucket.count)
    }, math.Max, 1))

    l.maxPASSCache.Store(&counterCache{val: rawMaxPass, time: time.Now()})
    return rawMaxPass
}

为什么要缓存？ reduce 需要遍历 100 个桶并加锁，而 maxPASS 在每个请求的 shouldDrop 中都会被调用。缓存有效期为一个桶时长（100ms），大幅降低热点路径开销。
math.Max + fallback=1：在所有桶的 count 中取最大值。fallback=1 确保即使没有流量，maxPASS 也不为 0，避免后续除零。
语义：过去 10s 内任意一个 100ms 窗口中通过的最大请求数。代表系统的峰值吞吐能力。

3.2 timespan — 时间跨度计算
func (l *BBR) timespan(lastTime time.Time) int {
    v := int(time.Since(lastTime) / l.bucketDuration)
    if v > -1 { return v }
    return l.opts.Bucket  // 时钟回拨保护
}

返回自 lastTime 以来经过了多少个桶时长。
< 1 表示还在同一个桶内，缓存有效。
时钟回拨时 v 可能为负，返回 Bucket 数强制缓存失效。

3.3 minRT — 滑动窗口最小响应时间
func (l *BBR) minRT() int64 {
    rtCache := l.minRtCache.Load()
    if rtCache != nil {
        rc := rtCache.(*counterCache)
        if l.timespan(rc.time) < 1 {
            return rc.val
        }
    }
    rawRT := l.rtStat.reduce(func(bucket counterBucket) float64 {
        if bucket.count == 0 { return math.MaxFloat64 }  // 空桶不参与最小值计算
        return bucket.sum / float64(bucket.count)        // 桶内平均 RT
    }, math.Min, math.MaxFloat64)

    rawMinRT := int64(1)  // 最小保底 1ms，防除零
    if rawRT > 0 && rawRT != math.MaxFloat64 {
        rawMinRT = int64(math.Ceil(rawRT))
    }
    l.minRtCache.Store(&counterCache{val: rawMinRT, time: time.Now()})
    return rawMinRT
}

桶内先平均，桶间取最小：不是取所有请求的全局最小 RT，而是取"各桶平均 RT 的最小值"。这比全局最小值更稳定，不受个别极端快请求的影响。
fallback=math.MaxFloat64：math.Min 聚合时，空桶返回 MaxFloat64 不会被选为最小值。
Ceil + 保底 1ms：RT 向上取整避免低估并发容量。保底 1ms 防止 maxInFlight 计算结果为 0。

3.4 maxInFlight — Little's Law 核心公式
func (l *BBR) maxInFlight() int64 {
    return int64(math.Floor(float64(l.maxPASS(l.minRT()l.bucketPerSecond)/1000.0) + 0.5)
}

这就是 Little's Law: L = λ × W
符号   代码对应   含义
L   maxInFlight   系统能承载的最大并发数

λ   maxPASS × bucketPerSecond   每秒最大吞吐量（桶/s × 请求/桶）

W   minRT / 1000   最小响应时间（ms → s）

除以 1000：minRT 单位是 ms，需要转为秒以匹配吞吐量的"每秒"维度。
Floor + 0.5：等价于四舍五入。纯 Floor 会系统性低估容量。
直觉理解：如果系统峰值每秒处理 1000 个请求，每个请求最快 10ms，那么稳态下系统中同时存在的请求数 = 1000 × 0.01 = 10。超过 10 个并发就意味着排队/过载。

限流决策 shouldDrop

func (l *BBR) shouldDrop() bool {
    now := time.Duration(time.Now().UnixNano())

    // === 分支 A：CPU 未达阈值 ===
    if l.cpu() < l.opts.CPUThreshold {
        prevDropTime, _ := l.prevDropTime.Load().(time.Duration)
        if prevDropTime == 0 {
            return false  // 从未限流过，直接放行
        }
        // 冷却期：上次限流后 1s 内仍检查并发数
        if now-prevDropTime <= time.Second {
            inFlight := atomic.LoadInt64(&l.inFlight)
            return inFlight > 1 && inFlight > l.maxInFlight()
        }
        // 冷却期结束，清除限流标记
        l.prevDropTime.Store(time.Duration(0))
        return false
    }

    // === 分支 B：CPU 已达阈值 ===
    inFlight := atomic.LoadInt64(&l.inFlight)
    drop := inFlight > 1 && inFlight > l.maxInFlight()
    if drop {
        prevDrop, _ := l.prevDropTime.Load().(time.Duration)
        if prevDrop != 0 {
            return true  // 已在限流状态，持续拒绝
        }
        l.prevDropTime.Store(now)  // 首次触发，记录时间
    }
    return drop
}

🔑 关键设计解读

inFlight > 1 的保护：当并发数为 0 或 1 时永远不限流。这保证了即使在极低流量下也能有请求通过，避免"死锁式限流"——如果完全拒绝所有请求，RT 和 PASS 统计将永远无法更新，限流器永远无法恢复。

冷却机制（Cooldown）：CPU 从高降到低后，不立即完全放开，而是保持 1 秒的观察期。原因：
    CPU 采样有 500ms 延迟 + EMA 平滑滞后，CPU 读数下降不代表系统已真正恢复
    突然全量放行可能导致二次过载（类似 TCP 慢启动的思想）
    1 秒内继续用 maxInFlight 约束，给系统缓冲时间

prevDropTime 的双重作用：
    作为冷却计时器
    作为"已在限流状态"的标志（非零 = 正在限流）
    使用 atomic.Value 存储 time.Duration，避免额外锁

为什么不直接用 CPU 做开关？ 纯 CPU 阈值会导致"锯齿效应"：CPU 80% → 限流 → CPU 降到 79% → 放开 → CPU 又飙到 81% → 限流...。结合 maxInFlight 后，即使 CPU 超阈值，只要并发数没超 Little's Law 上限，仍然放行。这让限流更平滑。

Allow 入口与 DoneFunc 回调

func (l *BBR) Allow() (DoneFunc, error) {
    if l.shouldDrop() {
        return nil, ErrLimitExceed
    }
    atomic.AddInt64(&l.inFlight, 1)     // 进入临界区前 +1
    start := time.Now()
    return func(DoneInfo) {
        // 请求完成后回调
        if rt := math.Ceil(float64(time.Since(start).Nanoseconds()) / float64(time.Millisecond)); rt > 0 {
            l.rtStat.add(rt)            // 记录 RT（ms）
        }
        atomic.AddInt64(&l.inFlight, -1) // 离开临界区 -1
        l.passStat.add(1)               // 记录一次成功通过
    }, nil
}

inFlight 的原子性：AddInt64(+1) 在 shouldDrop 之后、实际处理之前。如果两个 goroutine 同时通过 shouldDrop，inFlight 会正确累加。下一个请求再检查时就能看到更新后的值。
DoneFunc 模式：限流器不侵入业务逻辑。调用方只需 defer done(DoneInfo{}) 即可自动完成统计。
RT 向上取整：亚毫秒级请求记为 1ms，配合 minRT 的保底 1ms，确保 Little's Law 计算不会因精度丢失而产生 0 值。
passStat 在 inFlight-1 之后记录：顺序不重要（两者独立），但语义上"完成"后才算一次有效通过。

RollingCounter 滑动窗口实现

6.1 数据结构
type rollingCounter struct {
    mu             sync.Mutex
    buckets        []counterBucket
    bucketDuration time.Duration
}

type counterBucket struct {
    slot  int64    // 时间槽编号（绝对时间 / bucketDuration）
    sum   float64  // 累计值（RT 求和 / PASS 计数累加）
    count int64    // 样本数
}

环形缓冲区：buckets 是固定大小的数组，通过 slot % len(buckets) 映射索引，避免内存分配。
slot 机制：不用定时器清理过期桶，而是在访问时通过 slot 比较判断桶是否属于当前窗口。惰性淘汰，零后台开销。

6.2 add — 写入
func (r *rollingCounter) add(value float64) {
    slot := r.currentSlot()
    offset := int(slot % int64(len(r.buckets)))

    r.mu.Lock()
    defer r.mu.Unlock()
    bucket := &r.buckets[offset]
    if bucket.slot != slot {    // 桶已过期或被复用
        bucket.slot = slot      // 重置为新时间段
        bucket.sum = 0
        bucket.count = 0
    }
    bucket.sum += value
    bucket.count++
}

懒重置：只有当写入时发现桶的 slot 不匹配当前时间，才清零。如果某个桶长时间没被访问，下次访问时自然重置，无需定时清理。
锁粒度：整个 Counter 一把互斥锁。对于 100 桶 × 高并发场景，可考虑分段锁或 atomic 优化，但当前实现在大多数微服务场景中足够。

6.3 reduce — 聚合查询
func (r *rollingCounter) reduce(
    value func(counterBucket) float64,
    aggregate func(float64, float64) float64,
    fallback float64,
) float64 {
    slot := r.currentSlot()
    size := int64(len(r.buckets))
    result := fallback

    r.mu.Lock()
    defer r.mu.Unlock()
    for _, bucket := range r.buckets {
        // 跳过无效桶：
        if bucket.count == 0 ||           // 空桶
           bucket.slot == slot ||         // 当前正在写入的桶（不完整）
           slot-bucket.slot >= size ||    // 超出窗口范围
           bucket.slot > slot {           // 未来时间（时钟异常）
            continue
        }
        result = aggregate(result, value(bucket))
    }
    return result
}

排除当前桶：bucket.slot == slot 的桶正在被写入，数据不完整，不参与聚合。这保证了统计结果的稳定性。
泛型聚合：通过函数参数实现 max/min/sum 等多种聚合，一套代码服务于 passStat 和 rtStat。
O(n) 遍历：n=Bucket=100，每次 reduce 遍历 100 个元素，配合缓存机制（100ms 有效期），实际 QPS 开销极低。

6.4 currentSlot
func (r *rollingCounter) currentSlot() int64 {
    return time.Now().UnixNano() / int64(r.bucketDuration)
}

将纳秒时间戳除以桶时长，得到单调递增的槽编号。
溢出安全：int64 纳秒约 292 年后溢出，实际无需担心。

📌 整体架构总结

请求到达
   │
   ▼
Allow() ──→ shouldDrop()?
   │              │
   │         ┌────┴────┐
   │      NO │         │ YES
   │         ▼         ▼
   │    inFlight++   return ErrLimitExceed
   │         │
   │    执行业务逻辑
   │         │
   │    DoneFunc()
   │    ├── rtStat.add(rt)
   │    ├── inFlight--
   │    └── passStat.add(1)
   │
   ▼
后台 cpuproc (500ms)
   └── EMA 平滑 → gCPU

shouldDrop 决策树:
   CPU < 阈值?
     ├─ YES → 冷却期内? → 检查 maxInFlight
     │         └─ 否 → 放行
     └─ NO  → inFlight > maxInFlight?
               ├─ YES → 拒绝
               └─ NO  → 放行

maxInFlight = maxPASS × minRT × bucketPerSecond / 1000
              (Little's Law: L = λ × W)

这个实现的精妙之处在于：它不是一个简单的开关，而是一个带惯性、带冷却、带数学模型的反馈控制系统。CPU 是触发信号，Little's Law 是约束边界，冷却机制是阻尼器，三者共同实现了平滑、自适应的限流效果。
*/

var (
	gCPU  int64
	decay = 0.95
)

type (
	cpuGetter func() int64

	// Option configures a BBR limiter.
	Option func(*options)
)

func init() {
	go cpuproc()
}

func cpuproc() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	sampler := newCPUSampler()
	for range ticker.C {
		curCPU := sampler.usage()
		if curCPU < 0 {
			continue
		}
		curCPU = minInt64(curCPU, 1000)
		prevCPU := atomic.LoadInt64(&gCPU)
		cpu := int64(float64(prevCPU)*decay + float64(curCPU)*(1.0-decay))
		atomic.StoreInt64(&gCPU, cpu)
	}
}

type cpuSampler struct {
	mu        sync.Mutex
	samples   []metrics.Sample
	prevUsed  float64
	prevTotal float64
	ready     bool
}

func newCPUSampler() *cpuSampler {
	return &cpuSampler{
		samples: []metrics.Sample{
			{Name: "/cpu/classes/total:cpu-seconds"},
			{Name: "/cpu/classes/idle:cpu-seconds"},
		},
	}
}

func (s *cpuSampler) usage() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()

	metrics.Read(s.samples)
	total := s.samples[0].Value.Float64()
	idle := s.samples[1].Value.Float64()
	used := total - idle
	if !s.ready {
		s.prevUsed = used
		s.prevTotal = total
		s.ready = true
		return 0
	}
	usedDelta := used - s.prevUsed
	totalDelta := total - s.prevTotal
	s.prevUsed = used
	s.prevTotal = total
	if totalDelta <= 0 || usedDelta < 0 {
		return 0
	}
	return int64((usedDelta / totalDelta) * 1000)
}

func minInt64(l, r int64) int64 {
	if l < r {
		return l
	}
	return r
}

// Stat contains a BBR metrics snapshot.
type Stat struct {
	CPU         int64
	InFlight    int64
	MaxInFlight int64
	MinRt       int64
	MaxPass     int64
}

type counterCache struct {
	val  int64
	time time.Time
}

type options struct {
	Window       time.Duration
	Bucket       int
	CPUThreshold int64
	CPUQuota     float64
}

// WithWindow sets the rolling window duration.
func WithWindow(d time.Duration) Option {
	return func(o *options) {
		o.Window = d
	}
}

// WithBucket sets the rolling window bucket count.
func WithBucket(b int) Option {
	return func(o *options) {
		o.Bucket = b
	}
}

// WithCPUThreshold sets the CPU threshold, scaled from 0 to 1000.
func WithCPUThreshold(threshold int64) Option {
	return func(o *options) {
		o.CPUThreshold = threshold
	}
}

// WithCPUQuota sets the real CPU quota if it differs from GOMAXPROCS.
func WithCPUQuota(quota float64) Option {
	return func(o *options) {
		o.CPUQuota = quota
	}
}

// BBR implements a BBR-like adaptive limiter.
type BBR struct {
	cpu             cpuGetter
	passStat        *rollingCounter
	rtStat          *rollingCounter
	inFlight        int64
	bucketPerSecond int64
	bucketDuration  time.Duration

	prevDropTime atomic.Value
	maxPASSCache atomic.Value
	minRtCache   atomic.Value

	opts options
}

// NewLimiter returns a BBR limiter.
func NewLimiter(opts ...Option) *BBR {
	opt := options{
		Window:       10 * time.Second,
		Bucket:       100,
		CPUThreshold: 800,
	}
	for _, o := range opts {
		o(&opt)
	}
	if opt.Window <= 0 {
		opt.Window = 10 * time.Second
	}
	if opt.Bucket < 1 {
		opt.Bucket = 1
	}

	bucketDuration := opt.Window / time.Duration(opt.Bucket)
	if bucketDuration <= 0 {
		bucketDuration = opt.Window
	}

	limiter := &BBR{
		opts:            opt,
		passStat:        newRollingCounter(opt.Bucket, bucketDuration),
		rtStat:          newRollingCounter(opt.Bucket, bucketDuration),
		bucketDuration:  bucketDuration,
		bucketPerSecond: int64(time.Second / bucketDuration),
		cpu:             func() int64 { return atomic.LoadInt64(&gCPU) },
	}
	if limiter.bucketPerSecond < 1 {
		limiter.bucketPerSecond = 1
	}
	if opt.CPUQuota != 0 {
		limiter.cpu = func() int64 {
			return int64(float64(atomic.LoadInt64(&gCPU)) * float64(runtime.GOMAXPROCS(0)) / opt.CPUQuota)
		}
	}
	return limiter
}

func (l *BBR) maxPASS() int64 {
	passCache := l.maxPASSCache.Load()
	if passCache != nil {
		ps := passCache.(*counterCache)
		if l.timespan(ps.time) < 1 {
			return ps.val
		}
	}
	rawMaxPass := int64(l.passStat.reduce(func(bucket counterBucket) float64 {
		return float64(bucket.count)
	}, math.Max, 1))
	l.maxPASSCache.Store(&counterCache{
		val:  rawMaxPass,
		time: time.Now(),
	})
	return rawMaxPass
}

func (l *BBR) timespan(lastTime time.Time) int {
	v := int(time.Since(lastTime) / l.bucketDuration)
	if v > -1 {
		return v
	}
	return l.opts.Bucket
}

func (l *BBR) minRT() int64 {
	rtCache := l.minRtCache.Load()
	if rtCache != nil {
		rc := rtCache.(*counterCache)
		if l.timespan(rc.time) < 1 {
			return rc.val
		}
	}
	rawRT := l.rtStat.reduce(func(bucket counterBucket) float64 {
		if bucket.count == 0 {
			return math.MaxFloat64
		}
		return bucket.sum / float64(bucket.count)
	}, math.Min, math.MaxFloat64)
	rawMinRT := int64(1)
	if rawRT > 0 && rawRT != math.MaxFloat64 {
		rawMinRT = int64(math.Ceil(rawRT))
	}
	l.minRtCache.Store(&counterCache{
		val:  rawMinRT,
		time: time.Now(),
	})
	return rawMinRT
}

func (l *BBR) maxInFlight() int64 {
	return int64(math.Floor(float64(l.maxPASS()*l.minRT()*l.bucketPerSecond)/1000.0) + 0.5)
}

func (l *BBR) shouldDrop() bool {
	now := time.Duration(time.Now().UnixNano())
	if l.cpu() < l.opts.CPUThreshold {
		prevDropTime, _ := l.prevDropTime.Load().(time.Duration)
		if prevDropTime == 0 {
			return false
		}
		if now-prevDropTime <= time.Second {
			inFlight := atomic.LoadInt64(&l.inFlight)
			return inFlight > 1 && inFlight > l.maxInFlight()
		}
		l.prevDropTime.Store(time.Duration(0))
		return false
	}
	inFlight := atomic.LoadInt64(&l.inFlight)
	drop := inFlight > 1 && inFlight > l.maxInFlight()
	if drop {
		prevDrop, _ := l.prevDropTime.Load().(time.Duration)
		if prevDrop != 0 {
			return true
		}
		l.prevDropTime.Store(now)
	}
	return drop
}

// Stat returns a metrics snapshot.
func (l *BBR) Stat() Stat {
	return Stat{
		CPU:         l.cpu(),
		MinRt:       l.minRT(),
		MaxPass:     l.maxPASS(),
		MaxInFlight: l.maxInFlight(),
		InFlight:    atomic.LoadInt64(&l.inFlight),
	}
}

/*
inFlight 这个词的直译是“正在飞行中”。在 BBR 限流器的语境下，它代表的是 “当前正在被处理、尚未返回结果的请求数量”（即实时并发数）。

减去 1 的根本意义在于：准确追踪系统当前的真实负载水位，为 Little's Law 公式提供正确的输入。

我们可以从以下三个层面来理解这个 -1 的意义：

物理语义：资源的“借”与“还”
把系统的处理能力想象成一个有固定座位数的餐厅：
Allow() 中的 +1：相当于一个顾客进店坐下，占用了一个座位。此时 inFlight 增加，表示系统多承担了一份负载。
业务处理过程：顾客正在吃饭（请求正在执行），座位一直被占着。
DoneFunc 中的 -1：相当于顾客吃完饭离开，归还了座位。此时 inFlight 减少，表示系统释放了一份负载，恢复了相应的处理能力。

如果不减 1，就意味着顾客吃完饭后“人走了但座位还被标记为占用”，座位会被永久锁死。

数学语义：Little's Law 的动态平衡
BBR 的核心判据是 inFlight > maxInFlight。其中 maxInFlight 是通过 Little's Law (L = λ × W) 算出的理论上限。

maxInFlight 是一个动态变化的阈值（随 maxPASS 和 minRT 波动）。
inFlight 是一个实时变化的观测值。

-1 的作用是让观测值紧跟实际状态。只有当 inFlight 精确反映“此刻真正在处理中的请求数”时，inFlight > maxInFlight 这个不等式才有物理意义：
不减 1 → inFlight 单调递增 → 很快永远大于 maxInFlight → 限流器退化为永久熔断器，服务彻底不可用。
正确减 1 → inFlight 随请求完成而回落 → 当负载降低时，新请求能重新通过检查 → 限流器具备自愈能力。

控制论语义：负反馈闭环
BBR 本质上是一个自动控制系统。任何稳定的控制系统都必须同时具备正反馈和负反馈：
操作   控制论角色   作用
inFlight++   正反馈   负载上升信号，推动系统趋向限流边界

inFlight--   负反馈   负载下降信号，拉回系统远离限流边界

-1 就是这个负反馈信号。它告诉控制器：“压力已经减轻了，你可以适当放宽闸门。” 没有负反馈的系统是不稳定的——就像一辆只有油门没有刹车的车，要么不动，要么撞毁。

💡 一句话总结

+1 是声明“我正在消耗资源”，-1 是声明“我已经归还资源”。 两者配对，才能让 inFlight 成为一面忠实反映系统实时负载的镜子，而不是一个只增不减的死亡计数器。
*/

// Allow checks whether a request is allowed.
func (l *BBR) Allow() (DoneFunc, error) {
	if l.shouldDrop() {
		return nil, ErrLimitExceed
	}
	atomic.AddInt64(&l.inFlight, 1)
	start := time.Now()
	return func(DoneInfo) {
		if rt := math.Ceil(float64(time.Since(start).Nanoseconds()) / float64(time.Millisecond)); rt > 0 {
			l.rtStat.add(rt)
		}
		atomic.AddInt64(&l.inFlight, -1)
		l.passStat.add(1)
	}, nil
}

type rollingCounter struct {
	mu             sync.Mutex
	buckets        []counterBucket
	bucketDuration time.Duration
}

type counterBucket struct {
	slot  int64
	sum   float64
	count int64
}

func newRollingCounter(size int, bucketDuration time.Duration) *rollingCounter {
	return &rollingCounter{
		buckets:        make([]counterBucket, size),
		bucketDuration: bucketDuration,
	}
}

func (r *rollingCounter) add(value float64) {
	slot := r.currentSlot()
	offset := int(slot % int64(len(r.buckets)))

	r.mu.Lock()
	defer r.mu.Unlock()
	bucket := &r.buckets[offset]
	if bucket.slot != slot {
		bucket.slot = slot
		bucket.sum = 0
		bucket.count = 0
	}
	bucket.sum += value
	bucket.count++
}

func (r *rollingCounter) reduce(value func(counterBucket) float64, aggregate func(float64, float64) float64, fallback float64) float64 {
	slot := r.currentSlot()
	size := int64(len(r.buckets))
	result := fallback

	r.mu.Lock()
	defer r.mu.Unlock()
	for _, bucket := range r.buckets {
		if bucket.count == 0 || bucket.slot == slot || slot-bucket.slot >= size || bucket.slot > slot {
			continue
		}
		result = aggregate(result, value(bucket))
	}
	return result
}

func (r *rollingCounter) currentSlot() int64 {
	return time.Now().UnixNano() / int64(r.bucketDuration)
}
