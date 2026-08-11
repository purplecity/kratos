package log

import (
	"context"
	"log/slog"
	"runtime"
	"time"
)

/*
slog 是 Go 1.21 引入的标准库结构化日志包（log/slog）。它的出现标志着 Go 官方终于结束了社区在结构化日志上“百花齐放、各自为政”的局面（zap、zerolog、logrus 等），提供了一个统一、高性能、零依赖的标准方案。

下面从设计哲学、核心概念、使用方式到高级特性，为你彻底讲透。

🎯 为什么需要 slog？

在 slog 之前，Go 标准库的 log 包只能输出纯文本字符串：

// ❌ 旧 log 包：非结构化，难以机器解析
log.Printf("user %s login failed from %s, attempt %d", user, ip, attempt)
// 输出: 2026/08/11 07:51:00 user alice login failed from 10.0.0.1, attempt 3

而现代后端系统（ELK、Loki、Datadog）都需要 JSON 结构化日志。以前你必须引入第三方库，现在标准库原生支持：

// ✅ slog：结构化，机器友好
slog.Info("login failed", "user", user, "ip", ip, "attempt", attempt)
// JSON 输出: {"time":"2026-08-11T07:51:00Z","level":"INFO","msg":"login failed","user":"alice","ip":"10.0.0.1","attempt":3}

🏗️ 三大核心概念

slog 的架构非常简洁，由三个角色组成：

┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│   Logger     │────▶│   Handler    │────▶│  Output     │
│ (API 门面)   │     │ (格式化+过滤) │     │ (Writer)    │
└─────────────┘     └──────────────┘     └─────────────┘

概念   职责   类比
Logger   提供 Info/Warn/Error/Debug 等 API，持有上下文属性   你调用日志的对象

Handler   决定日志如何格式化（JSON/Text）、是否输出（Level过滤）   可替换的渲染引擎

Record   一条日志的内部表示（时间、级别、消息、属性列表）   Logger 和 Handler 之间的数据载体

💡 关键设计：Logger 和 Handler 是解耦的。同一个 Logger 可以随时切换 Handler，也可以在运行时动态组合多个 Handler。

🚀 快速上手

默认全局 Logger（开箱即用）
// 默认 Text 格式，输出到 os.Stderr
slog.Info("server started", "port", 8080)
slog.Error("db connection failed", "err", err, "retry", 3)

切换为 JSON 格式（生产环境必备）
logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
    Level: slog.LevelInfo,
}))
slog.SetDefault(logger) // 设为全局默认

slog.Info("request handled", "method", "GET", "path", "/api/users", "status", 200, "latency_ms", 42)
// {"time":"2026-08-11T07:51:00Z","level":"INFO","msg":"request handled","method":"GET","path":"/api/users","status":200,"latency_ms":42}

带上下文的子 Logger（避免重复传参）
reqLogger := logger.With("request_id", reqID, "user_id", userID)
reqLogger.Info("processing order")   // 自动带上 request_id 和 user_id
reqLogger.Warn("slow query", "sql", q, "duration_ms", 1200)

⚡ 高性能设计：Attr 与 LogValuer

这是 slog 相比很多第三方库的杀手级特性。

问题：日志被过滤时，参数仍然会被求值
// ❌ 即使 Debug 级别被关闭，fmt.Sprintf 仍然会执行！
slog.Debug("cache miss", "key", key, "detail", fmt.Sprintf("%+v", expensiveObj))

解决方案1：惰性求值（LogValuer）
type DBConn struct { }

func (c *DBConn) LogValue() slog.Value {
    return slog.StringValue(fmt.Sprintf("pool=%d/%d", c.Active(), c.Max()))
}

// 只有当日志真正要输出时，LogValue() 才会被调用
slog.Debug("db status", "conn", dbConn)

解决方案2：预构造 Attr（避免重复分配）
// 热点路径中，避免每次调用都创建 []any slice
attr := slog.Int("status", 200)
for _, req := range requests {
    logger.LogAttrs(ctx, slog.LevelInfo, "handled", attr, slog.String("path", req.Path))
}

LogAttrs 接受 ...slog.Attr 而非 ...any，避免了接口装箱和反射开销，是性能敏感场景的首选 API。

🔧 自定义 Handler（扩展能力）

slog 的真正威力在于 Handler 是可组合的。例如实现一个"脱敏 + 多输出"的 Handler：

// 包装已有 Handler，对敏感字段脱敏
type RedactHandler struct {
    inner slog.Handler
}

func (h *RedactHandler) Handle(ctx context.Context, r slog.Record) error {
    // 遍历属性，替换密码字段
    r.Attrs(func(a slog.Attr) bool {
        if a.Key == "password" {
            a.Value = slog.StringValue("REDACTED")
        }
        return true
    })
    return h.inner.Handle(ctx, r)
}

func (h *RedactHandler) Enabled(ctx context.Context, level slog.Level) bool {
    return h.inner.Enabled(ctx, level)
}
func (h *RedactHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
    return &RedactHandler{inner: h.inner.WithAttrs(attrs)}
}
func (h *RedactHandler) WithGroup(name string) slog.Handler {
    return &RedactHandler{inner: h.inner.WithGroup(name)}
}

社区已经涌现大量现成 Handler：lumberjack（日志轮转）、otel（OpenTelemetry 集成）、multi（同时写文件+stdout）等。

📊 slog vs 第三方库选型建议
场景   推荐
新项目 / 微服务 / 不想引入外部依赖   ✅ slog

极致性能（纳秒级热路径）   zerolog（仍略快于 slog）

已有大量 zap/logrus 代码的老项目   继续用原库，或用 slogbridge 适配

需要采样、CallerSkip 精细控制   zap

需要与 OpenTelemetry 无缝集成   slog + otelslog Handler

💡 趋势判断：Go 生态正在向 slog 收敛。Kratos、Gin、gRPC-Gateway 等主流框架已陆续支持 slog 作为日志接口。新项目强烈建议直接使用 slog。

📌 一句话总结

slog = Go 官方的结构化日志标准，以 Logger/Handler 分离架构为核心，兼顾易用性与高性能，通过 LogValuer 和 LogAttrs 解决惰性求值与零分配问题，并通过可组合 Handler 实现无限扩展。

如果你有具体的使用场景（比如如何在 Kratos 中集成 slog、如何做日志采样、如何对接 Loki 等），可以继续问，我可以给出针对性方案。


slog 的并发安全性需要分两部分看，不能一概而论：

✅ 安全的部分：Logger 的使用（读操作）

一旦 Logger 被创建并设置好，以下操作在任意多个 goroutine 中并发调用都是完全安全的：

logger.Info() / Warn() / Error() / Debug()
logger.With() / WithGroup() （返回新的 Logger，不修改原 Logger）
slog.Info() 等全局函数（读取 defaultLogger）

这是因为 *slog.Logger 本身是不可变设计：
With() 不会修改原 Logger，而是 clone 一个新的
Handler 接口要求实现必须是并发安全的
内置的 JSONHandler / TextHandler 内部对 Writer 加了 mutex

// ✅ 随便并发，没问题
var logger *slog.Logger // 初始化一次后不再变

func handleRequest(w http.ResponseWriter, r *http.Request) {
    logger.Info("request", "path", r.URL.Path) // 1000 个 goroutine 同时调也安全
}

⚠️ 不安全 / 需要注意的部分

SetDefault 本身不是原子操作
// ❌ 不要在运行时并发调用 SetDefault
go func() { slog.SetDefault(loggerA) }()
go func() { slog.SetDefault(loggerB) }() // 数据竞争！

最佳实践：只在 main() 或 init() 中调用一次。如果测试中需要替换，用 t.Cleanup(slog.SetDefault(old)) 串行恢复。

自定义 Handler 的实现必须自己保证并发安全
slog.Handler 接口的契约明确要求：Handle/Enabled/WithAttrs/WithGroup 必须可被多 goroutine 并发调用。如果你写的自定义 Handler 里有可变状态却忘了加锁，那就是你自己的 bug，不是 slog 的问题。

// ❌ 错误的自定义 Handler
type BadHandler struct {
    count int // 无保护的可变状态
}
func (h *BadHandler) Handle(ctx context.Context, r slog.Record) error {
    h.count++ // 💥 并发写入，data race
    return nil
}

// ✅ 正确做法
type GoodHandler struct {
    mu    sync.Mutex
    count int
}
func (h *GoodHandler) Handle(ctx context.Context, r slog.Record) error {
    h.mu.Lock()
    h.count++
    h.mu.Unlock()
    return nil
}

LogValuer.LogValue() 可能被并发调用
如果你的 LogValuer 实现访问了共享可变状态，也需要自行加锁。slog 不保证 LogValue() 的串行调用。

底层 Writer 的线程安全
JSONHandler / TextHandler 内部会对传入的 io.Writer 加互斥锁来保证每条日志完整写入。但如果你用 slog.NewJSONHandler(writer, nil) 传入的 writer 本身有特殊语义（比如某些网络 conn），要注意 handler 的锁粒度是单条 Record 级别，不是全局序列化。

📋 速查表
操作   并发安全？   备注
logger.Info/Warn/Error/Debug   ✅   核心使用场景，放心并发

logger.With() / WithGroup()   ✅   返回新对象，不改原对象

slog.FromContext(ctx)   ✅   纯读取

slog.SetDefault()   ❌   仅启动时调一次

内置 Handler (JSON/Text)   ✅   官方已处理

自定义 Handler   ⚠️   你必须自己保证

LogValuer.LogValue()   ⚠️   若访问共享状态需自行加锁

💡 一句话总结

用 slog 记日志 = 并发安全；改 slog 的配置/实现 = 你自己负责并发安全。 正常使用（Info/Warn/Error + With）在任何高并发场景下都不需要额外加锁，这也是它替代 fmt.Printf 和旧 log 包的重要原因之一。


这两个是 slog 中结构化能力和性能优化的核心机制，分别解决“日志怎么组织”和“日志怎么高效生成”两个问题。

📦 WithGroup：给日志属性加命名空间

它是什么
WithGroup(name) 返回一个新 Logger，后续该 Logger 输出的所有属性都会被嵌套在 name 这个 key 下面，形成层级结构。

直观对比
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

// ❌ 没有 Group：属性平铺，容易命名冲突
logger.Info("request done",
    "handler", "GetUser",
    "latency_ms", 42,
    "db_query", "SELECT * FROM users",
    "db_latency_ms", 12,
)
// {"msg":"request done","handler":"GetUser","latency_ms":42,"db_query":"SELECT...","db_latency_ms":12}

// ✅ 有 Group：语义清晰，天然隔离
reqLogger := logger.WithGroup("http")
dbLogger  := reqLogger.WithGroup("db")

reqLogger.Info("request done", "handler", "GetUser", "latency_ms", 42)
dbLogger.Info("query executed", "query", "SELECT * FROM users", "latency_ms", 12)

JSON 输出：
{"msg":"request done","http":{"handler":"GetUser","latency_ms":42}}
{"msg":"query executed","http":{"db":{"query":"SELECT * FROM users","latency_ms":12}}}

关键行为细节
特性   说明
不可变   WithGroup 返回新 Logger，原 Logger 不受影响

可嵌套   多次调用 WithGroup 会层层嵌套

与 With 的顺序   With 的属性归属到当前最近的 Group 中

空 Group 名   WithGroup("") 等同于无操作（不会创建空 key）

TextHandler   Text 格式用点号拼接：http.handler=GetUser

JSONHandler   JSON 格式生成真正的嵌套对象

⚠️ 常见陷阱：With 和 WithGroup 的顺序很重要
// A: With 在 WithGroup 之前 → attr 属于根级别
logger.With("version", "v2").WithGroup("http").Info("msg", "path", "/api")
// {"version":"v2","http":{"path":"/api"}}

// B: With 在 WithGroup 之后 → attr 属于 http 组
logger.WithGroup("http").With("version", "v2").Info("msg", "path", "/api")
// {"http":{"version":"v2","path":"/api"}}

💡 记忆法则：WithGroup 像打开一个文件夹，之后的 With 和日志参数都放进这个文件夹里，直到遇到下一个 WithGroup 或回到上层。

典型使用场景
中间件链：WithGroup("auth"), WithGroup("cache"), WithGroup("db")
子系统划分：避免不同模块的同名 key 互相覆盖
对接日志平台：Elasticsearch/Loki 对嵌套字段有更好的索引和查询支持

⚡ LogValuer：惰性求值接口

它解决什么问题
// ❌ 即使 Debug 被关闭，expensiveDebugInfo() 仍然会执行！
slog.Debug("cache state", "detail", expensiveDebugInfo())

函数参数在 Go 中是立即求值的。当日志级别被过滤掉时，你白白付出了序列化/查询/计算的开销。在高并发热路径上，这可能是严重的性能瓶颈。

接口定义
type LogValuer interface {
    LogValue() Value
}

任何实现了这个接口的类型，在被 slog 处理时，只有在日志确实要输出时才会调用 LogValue()。

完整示例
type PoolStats struct {
    pool *ConnectionPool
}

// 只有日志真正要写出去时，才去查连接池状态
func (p PoolStats) LogValue() slog.Value {
    active, idle := p.pool.Stats() // 可能有锁竞争或系统调用
    return slog.GroupValue(
        slog.Int("active", active),
        slog.Int("idle", idle),
        slog.Float64("utilization", float64(active)/float64(active+idle)),
    )
}

// 使用：无论什么级别，传参成本都是 O(1)
logger.Debug("pool status", "stats", PoolStats{pool: dbPool})
logger.Info("pool status", "stats", PoolStats{pool: dbPool}) // Info 开启时才真正调 LogValue()

执行时机图解
slog.Debug("msg", "key", someLogValuer)
         │
         ▼
   Handler.Enabled(Debug)?
      │           │
     YES          NO → 直接丢弃，LogValue() 永远不被调用 ✅
      │
      ▼
  构造 Record
      │
      ▼
  遍历 Attrs → 发现 LogValuer → 调用 LogValue() → 替换为真实 Value
      │
      ▼
  Handler.Handle(record)

⚠️ 重要注意事项
注意点   说明
可能被多次调用   如果多个 Handler 包装了同一个 Record，LogValue() 可能被调多次。不要在里面做有副作用的操作（如自增计数器）

并发安全自负   slog 不保证串行调用 LogValue()，若访问共享状态需自行加锁

返回值应是纯 Value   不要在 LogValue() 里再返回另一个 LogValuer（虽然技术上允许递归解析，但容易造成意外开销）

不适合简单类型   int, string 等本身就没有求值成本，没必要包一层 LogValuer

典型使用场景
数据库连接池 / 线程池状态：查 stats 有锁开销
大对象摘要：只在需要时才序列化
远程配置 / Feature Flag：查一次 RPC 才能拿到值
请求体 dump：Debug 时才读取 body，Info 时跳过

🤝 两者配合使用

// Group 提供结构，LogValuer 提供惰性
dbLogger := logger.WithGroup("database")

dbLogger.Debug("connection pool",
    "stats", PoolStats{pool: dbPool},  // 惰性：Debug 关闭时零开销
    "config", dbConfig,                 // 普通值：始终求值
)

输出（Debug 开启时）：
{
  "level": "DEBUG",
  "msg": "connection pool",
  "database": {
    "stats": {"active": 5, "idle": 15, "utilization": 0.25},
    "config": {"max_open": 20, "timeout_ms": 3000}
  }
}

📌 一句话总结

WithGroup = 日志的目录结构，让属性有层次、不冲突
LogValuer = 日志的懒加载，让被过滤的日志零计算开销
一个管“怎么组织”，一个管“什么时候算”，合在一起就是 slog 既结构化又高性能的根本原因。
*/

// SetDefault sets the default logger used by the package-level helpers and by
// [slog.Default].
func SetDefault(logger *slog.Logger) {
	slog.SetDefault(logger)
}

// Default returns the default logger.
func Default() *slog.Logger {
	return slog.Default()
}

// With returns a logger that includes the given attributes in each output
// operation. It mirrors [slog.Logger.With] on the default logger.
func With(args ...any) *slog.Logger {
	return slog.With(args...)
}

// WithGroup returns a logger that starts a group. It mirrors
// [slog.Logger.WithGroup] on the default logger.
func WithGroup(name string) *slog.Logger {
	return Default().WithGroup(name)
}

// Handler returns the default logger's handler. It mirrors
// [slog.Logger.Handler] on the default logger.
func Handler() slog.Handler {
	return Default().Handler()
}

// Enabled reports whether the default logger emits log records at the given
// context and level. It mirrors [slog.Logger.Enabled] on the default logger.
func Enabled(ctx context.Context, level Level) bool {
	return Default().Enabled(ctx, level)
}

// Debug logs at debug level. Signature mirrors [slog.Logger.Debug].
func Debug(msg string, args ...any) {
	log(context.Background(), LevelDebug, msg, args...)
}

// DebugContext logs at debug level with the provided context.
func DebugContext(ctx context.Context, msg string, args ...any) {
	log(ctx, LevelDebug, msg, args...)
}

// Info logs at info level. Signature mirrors [slog.Logger.Info].
func Info(msg string, args ...any) {
	log(context.Background(), LevelInfo, msg, args...)
}

// InfoContext logs at info level with the provided context.
func InfoContext(ctx context.Context, msg string, args ...any) {
	log(ctx, LevelInfo, msg, args...)
}

// Warn logs at warn level. Signature mirrors [slog.Logger.Warn].
func Warn(msg string, args ...any) {
	log(context.Background(), LevelWarn, msg, args...)
}

// WarnContext logs at warn level with the provided context.
func WarnContext(ctx context.Context, msg string, args ...any) {
	log(ctx, LevelWarn, msg, args...)
}

// Error logs at error level. Signature mirrors [slog.Logger.Error].
func Error(msg string, args ...any) {
	log(context.Background(), LevelError, msg, args...)
}

// ErrorContext logs at error level with the provided context.
func ErrorContext(ctx context.Context, msg string, args ...any) {
	log(ctx, LevelError, msg, args...)
}

// Log emits a record at the given level. It mirrors [slog.Logger.Log] on the
// default logger.
func Log(ctx context.Context, level Level, msg string, args ...any) {
	log(ctx, level, msg, args...)
}

// LogAttrs emits a typed-attr record at the given level. It mirrors
// [slog.Logger.LogAttrs] on the default logger.
//
//nolint:revive // LogAttrs intentionally mirrors slog.Logger.LogAttrs.
func LogAttrs(ctx context.Context, level Level, msg string, attrs ...slog.Attr) {
	handler := slog.Default().Handler()
	if !handler.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	// Skip [runtime.Callers, LogAttrs].
	runtime.Callers(2, pcs[:])
	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	record.AddAttrs(attrs...)
	_ = handler.Handle(ctx, record)
}

/*
这是 slog 封装中最容易踩坑、也最核心的一个细节。

🎯 runtime.Callers 到底是什么？

runtime.Callers(skip, pc) 的作用是捕获当前 goroutine 的调用栈程序计数器（PC）。

pc：存放捕获到的 PC 值的切片
skip：跳过栈顶的 N 层函数帧（0 = runtime.Callers 自身）

捕获到 PC 后，传给 slog.NewRecord，Handler 内部会用 runtime.CallersFrames 将 PC 解析为 文件名 + 行号 + 函数名，最终输出到日志里。

🔢 skip 为什么有时是 2、有时是 3？

skip 的值完全取决于你的封装层数。每多包一层函数，skip 就要 +1。

用 Kratos 这段代码的调用链来演示：

用户代码:  log(ctx, INFO, "hello")        ← 你想打印这一行

	↓

封装层:    func log(...)                   ← skip=3 跳到这里之上

	↓
	runtime.Callers(3, pcs[:])      ← skip=0: 自己
	                                    skip=1: log()
	                                    skip=2: 用户代码 ✅ ← 目标
	                                    skip=3: 用户的调用者 ❌ (跳过了)

等等，Kratos 写的是 skip=3，注释说跳过 [runtime.Callers, log, exported helper]，这意味着 Kratos 的实际调用链是 4 层：

用户业务代码:  logger.Info("hello")         ← 你真正想看到的行

	↓

导出方法:      func (l *Logger) Info(...)   ← exported helper (skip=3)

	↓

内部统一入口:  func log(...)                ← (skip=2)

	↓
	runtime.Callers(3, pcs)     ← skip=0: 自己
	                                skip=1: log()
	                                skip=2: Info()
	                                skip=3: 用户业务代码 ✅

💡 核心公式：skip = runtime.Callers 到用户真实调用点之间的函数帧数量

常见封装层数对照表
封装方式   调用链深度   skip 值
直接用 slog.Info()   slog 内部已处理   不需要手动 Callers

1 层包装 func MyLog(...)   MyLog → 用户   2

2 层包装 Info() → log()   log → Info → 用户   3 (Kratos 的情况)

3 层包装   再多一层   4

⚠️ skip 设错的后果：
skip 太小：打印的是封装函数内部的行号（比如 log.go:42），毫无意义
skip 太大：打印的是用户代码的调用者的行号，定位错误

✅ 如何确保永远打印正确的文件和行号

方法 1：数清楚你的封装层数（最基本）

画出从 runtime.Callers 到用户调用点的完整调用链，逐层计数。每次重构封装结构时都要重新验证。

方法 2：写单元测试自动校验（强烈推荐）

不要靠人眼数，用测试锁定：

	func TestCallerSkip(t *testing.T) {
	    var buf bytes.Buffer
	    logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{
	        AddSource: true, // ⬅️ 关键：必须开启才会输出 source 字段
	    }))

	    // 模拟你的封装调用
	    myWrappedLog(logger, "test message") // ← 这行就是期望的行号

	    var m map[string]any
	    json.Unmarshal(buf.Bytes(), &m)

	    source := m["source"].(map[string]any)
	    file := filepath.Base(source["file"].(string))
	    line := int(source["line"].(float64))

	    // 断言：source 指向本文件的 TestCallerSkip 函数内
	    if file != "logger_test.go" || line == 0 {
	        t.Errorf("caller info wrong: got %s:%d", file, line)
	    }
	}

⚠️ 注意：AddSource: true 必须设置！默认 Handler 不会解析 PC，也就不会输出文件行号。很多人 skip 设对了但看不到 source，就是因为没开这个选项。

方法 3：使用 slog.Record.AddSource 辅助调试

开发阶段临时加一行，肉眼验证 skip 是否正确：

record := slog.NewRecord(time.Now(), level, msg, pcs[0])
// 临时调试：打印实际解析出的 caller
frames := runtime.CallersFrames(pcs[:])
frame, _ := frames.Next()
fmt.Printf("[DEBUG] resolved caller: %s:%d %sn", frame.File, frame.Line, frame.Function)

确认输出是你期望的用户代码位置后，删掉这段调试代码。

方法 4：Go 1.24+ 使用 slog.NewLogRecorder（未来方案）

Go 团队已经意识到 skip 计算的脆弱性，正在推进标准库级别的解决方案。如果你用的是 Go 1.24+，可以关注 slog 的新 API 来避免手动管理 skip。

📌 总结
问题   答案
runtime.Callers 干嘛的？   捕获调用栈 PC，用于解析出文件名和行号

skip=2 还是 3？   取决于封装层数，没有固定值

# Kratos 为什么是 3？   因为它的调用链是 Callers → log → Info → 用户代码，共 3 层

怎么保证正确？   开启 AddSource: true + 写单测校验 + 重构后重新验证

skip 错了会怎样？   日志里的文件行号指向封装内部或更上层，完全失去定位价值

🎯 黄金法则：skip 不是一个可以背下来的数字，它是你的封装架构的函数签名。改了封装结构，就必须改 skip。用测试守护它，而不是靠记忆。

pcs[0] 就是 runtime.Callers 捕获到的第一个（也是最相关的）程序计数器（PC）值。

要理解它，需要把 runtime.Callers 的返回值拆开看：

🔍 pcs 是什么？

var pcs [1]uintptr
runtime.Callers(3, pcs[:])

pcs 是一个长度为 1 的数组，类型是 [1]uintptr
uintptr 是一个整数，代表内存中某个函数调用点的地址（Program Counter，程序计数器）
runtime.Callers(skip, pcs) 会从当前栈帧开始，跳过 skip 层后，把后续调用栈的 PC 值依次填入 pcs 切片中

因为这里只分配了 [1]uintptr，所以最多只会捕获 1 个 PC 值，也就是 pcs[0]。

🎯 pcs[0] 具体指向哪里？

结合前面的 skip=3 分析：

栈帧（从下往上）          说明
─────────────────────────────────
runtime.Callers         ← skip=0，跳过
log()                   ← skip=1，跳过
Info()                  ← skip=2，跳过
用户业务代码             ← skip=3，✅ pcs[0] 就是这一行的 PC
用户的调用者             ← 不会被捕获（数组只有1个元素）

pcs[0] = 跳过指定层数后，第一个被捕获的调用点地址 = 用户真实写日志的那行代码的地址

📌 为什么只取 [0] 而不是更多？

slog.NewRecord 的签名是：

func NewRecord(t time.Time, level Level, msg string, pc uintptr) Record

它只需要 一个 PC 值来确定日志的来源位置（文件 + 行号）。一条日志只有一个"产生位置"，不需要整个调用栈。

如果你需要完整的调用栈（比如 panic 堆栈），才会用更大的数组或 runtime.Stack()。但对于日志定位来说，pcs[0] 这一个就够了。

⚠️ 边界情况：pcs[0] 可能是 0

如果 runtime.Callers 实际捕获到的帧数 < 你请求的数量（比如 skip 设太大，栈不够深），pcs[0] 会是零值 0。

slog 内部对此有防御性处理：

// slog 源码简化版

	if record.PC != 0 {
	    frames := runtime.CallersFrames([]uintptr{record.PC})
	    frame, _ := frames.Next()
	    // 解析出 file, line, function
	} else {

	    // PC=0，不输出 source 信息
	}

所以即使 skip 设错导致 pcs[0] == 0，也不会 panic，只是日志里不会有文件和行号。

💡 一句话总结

pcs[0] = 跳过封装层后，用户写日志那行代码的内存地址。slog 拿到这个地址后，通过 runtime.CallersFrames 反查出文件名和行号，最终输出到日志的 source 字段中。整个机制就是：捕获地址 → 传入 Record → Handler 解析 → 输出位置信息。
*/
func log(ctx context.Context, level Level, msg string, args ...any) {
	handler := slog.Default().Handler()
	if !handler.Enabled(ctx, level) {
		return
	}
	var pcs [1]uintptr
	// Skip [runtime.Callers, log, exported helper].
	runtime.Callers(3, pcs[:])
	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	record.Add(args...)
	_ = handler.Handle(ctx, record)
}
