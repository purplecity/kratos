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
