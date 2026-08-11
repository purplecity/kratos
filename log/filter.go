package log

import (
	"context"
	"log/slog"
	"strings"
)

/*
这行代码 next := *h 是 Go 语言中实现 slog Handler 不可变语义 和 避免并发数据竞争 的核心惯用法。

要彻底理解它，需要明白三个层面的问题：

为什么不能直接修改 h？
slog.Handler 接口的设计契约要求：WithAttrs 和 WithGroup 必须返回一个新的 Handler，绝不能修改原 Handler。

因为一个 Logger（及其底层 Handler）会被多个 goroutine 并发共享：
// 全局 logger，被无数请求并发使用
logger.Info("request", "path", "/api")

// 如果 WithAttrs 修改了原 handler，上面那行并发日志的输出就会被污染！
userLogger := logger.With("user_id", 123)
userLogger.Info("login")

如果你写成这样：
// ❌ 致命错误：修改了共享的 h
func (h *filterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
    h.next = h.next.WithAttrs(attrs) // 💥 并发写入，data race + 污染原 logger
    return h
}

next := *h 到底做了什么？
这是 Go 的结构体值拷贝（shallow copy）：

next := *h  // 在栈上创建一个全新的 filterHandler 副本

字段   拷贝行为   安全性
cfg *filterConfig   拷贝指针（浅拷贝）   ✅ 安全：config 创建后只读，多 Handler 共享同一个 config 没问题

groups []string   拷贝 slice header（指向同一底层数组）   ⚠️ 需注意：但紧接着 WithGroup 里会 append([]string{}, ...) 创建新切片

next slog.Handler   拷贝接口值   ✅ 安全：马上就被 h.next.WithAttrs() 的返回值覆盖

拷贝之后，对 next 的所有修改都只影响这个新副本，原 h 完全不受影响。

为什么不直接用 &filterHandler{...} 显式构造？
你完全可以这么写，效果完全等价：

func (h *filterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
    if h.needsRewrite() {
        attrs = h.redactAttrs(h.groups, attrs)
    }
    return &filterHandler{
        next:   h.next.WithAttrs(attrs),
        cfg:    h.cfg,      // 共享只读配置
        groups: h.groups,   // 这里也安全，因为 WithAttrs 不修改 groups
    }
}

Kratos 选择 next := *h 的原因纯粹是工程维护性：

**当 filterHandler 新增字段时，*h 拷贝会自动包含新字段，而显式构造需要你手动记得加上去。** 忘了加某个字段 = 静默丢失状态 = 难以排查的 bug。

这在 slog 自定义 Handler 社区中是公认的最佳实践。Go 官方 slog 源码中的 JSONHandler、TextHandler 内部也是同样的写法。

🔍 这段代码还有一个精妙细节

注意看 WithAttrs 和 WithGroup 中 groups 的处理差异：

// WithAttrs：不修改 groups，所以值拷贝的 slice header 共享底层数组是安全的
func (h *filterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
    next := *h  // groups 共享底层数组，OK，因为没人会改它
    next.next = h.next.WithAttrs(attrs)
    return &next
}

// WithGroup：必须创建新切片！否则 append 可能修改原 handler 的底层数组
func (h *filterHandler) WithGroup(name string) slog.Handler {
    next := *h
    next.groups = append(append([]string{}, h.groups...), name) // ✅ 深拷贝 + 追加
    next.next = h.next.WithGroup(name)
    return &next
}

如果 WithGroup 里写成 next.groups = append(next.groups, name)，由于 next := *h 只是浅拷贝 slice header，append 在原容量范围内会直接修改原 h.groups 的底层数组，导致并发 bug。Kratos 用 append([]string{}, h.groups...) 显式深拷贝避免了这个问题。

📌 一句话总结

next := *h = 廉价地克隆一个 Handler 副本，确保 WithAttrs/WithGroup 的不可变语义，既避免并发数据竞争，又比显式构造更抗重构。这是写 slog 自定义 Handler 的标准范式，看到它就知道作者懂 slog 的设计哲学。

是的，完全正确。

h.next.WithAttrs(attrs) 会返回一个全新的 Handler 实例，这个新实例：

继承了 h.next 原有的所有属性和配置
追加了 传入的 attrs
不会修改 原来的 h.next

🔗 链式委托的本质

filterHandler 是一个装饰器（Decorator），它自己并不真正输出日志，而是把实际工作委托给内层的 next Handler。整个调用链看起来像这样：

filterHandler(脱敏) → JSONHandler(序列化) → Writer(输出)

当你调用 filterHandler.WithAttrs(...) 时，实际上发生了两层创建：

func (h *filterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
    // 第1层：filterHandler 克隆自己（next := *h）
    next := *h

    // 第2层：内层 Handler 也克隆自己，并加上新属性
    next.next = h.next.WithAttrs(attrs)  // ← 这里返回的是新的 JSONHandler

    return &next  // 返回新的 filterHandler，包裹着新的 JSONHandler
}

所以最终结果是：
对象   是否新建   说明
外层 filterHandler   ✅ 新建   next := *h 拷贝而来

内层 JSONHandler   ✅ 新建   h.next.WithAttrs() 返回的新实例

原始 h   ❌ 不变   完全未被修改

原始 h.next   ❌ 不变   完全未被修改

⚠️ 一个容易误解的点

"有相同的属性" ≠ "共享同一个属性存储"

新 Handler 拥有逻辑上相同的属性，但底层实现通常是复制了一份属性列表（或追加到新切片中），而不是共享引用。这正是为了保证不可变性——如果新旧 Handler 共享属性存储，后续对其中一个的操作就可能影响另一个。

以 Go 标准库 JSONHandler 源码为例：

func (h *JSONHandler) WithAttrs(attrs []Attr) Handler {
    if len(attrs) == 0 {
        return h  // 优化：没有新属性时直接返回自身
    }
    h2 := *h                          // 拷贝
    h2.preformatted = h.preformatted   // 复用已序列化的字节（只读，安全）
    h2.unprocessed = append([]Attr(nil), h.unprocessed...) // ✅ 深拷贝
    h2.unprocessed = append(h2.unprocessed, attrs...)      // 追加新属性
    return &h2
}

📌 总结

h.next.WithAttrs(attrs) = 让内层 Handler 带着原有属性 + 新属性生成一个新副本。外层 filterHandler 再把这个新副本包起来，形成一条全新的、独立的 Handler 链。整条链从外到内都是新的，原始链路完好无损，可以安全地继续被其他 goroutine 并发使用。

这就是 slog Handler 设计的精髓：**每次 With* 都是一次不可变的链式派生**，类似函数式编程中的持久化数据结构思想。
*/

const redactedValue = "***"

// FilterOption configures filtering in [WithFilter].
type FilterOption func(*filterConfig)

type filterConfig struct {
	keys   map[string]struct{}
	filter func(ctx context.Context, record slog.Record) bool
}

// FilterKey redacts the values of attributes whose key matches any of the
// provided keys. Keys may be leaf names ("password") or dotted group paths
// ("user.password").
func FilterKey(keys ...string) FilterOption {
	return func(c *filterConfig) {
		if c.keys == nil {
			c.keys = make(map[string]struct{}, len(keys))
		}
		for _, k := range keys {
			c.keys[k] = struct{}{}
		}
	}
}

// FilterFunc drops records for which fn returns true. fn is evaluated after key
// redaction.
func FilterFunc(fn func(ctx context.Context, record slog.Record) bool) FilterOption {
	return func(c *filterConfig) { c.filter = fn }
}

func newFilterHandler(next slog.Handler, opts ...FilterOption) slog.Handler {
	if next == nil {
		next = discardHandler{}
	}
	cfg := &filterConfig{}
	for _, o := range opts {
		o(cfg)
	}
	if len(cfg.keys) == 0 && cfg.filter == nil {
		return next
	}
	return &filterHandler{next: next, cfg: cfg}
}

type filterHandler struct {
	next   slog.Handler
	cfg    *filterConfig
	groups []string
}

func (h *filterHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *filterHandler) Handle(ctx context.Context, record slog.Record) error {
	if h.needsRewrite() {
		record = h.rewrite(record)
	}
	if h.cfg.filter != nil && h.cfg.filter(ctx, record) {
		return nil
	}
	return h.next.Handle(ctx, record)
}

func (h *filterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if h.needsRewrite() {
		attrs = h.redactAttrs(h.groups, attrs)
	}
	next := *h
	next.next = h.next.WithAttrs(attrs)
	return &next
}

func (h *filterHandler) WithGroup(name string) slog.Handler {
	next := *h
	next.groups = append(append([]string{}, h.groups...), name)
	next.next = h.next.WithGroup(name)
	return &next
}

func (h *filterHandler) needsRewrite() bool {
	return len(h.cfg.keys) > 0
}

func (h *filterHandler) rewrite(record slog.Record) slog.Record {
	cloned := record.Clone()
	attrs := make([]slog.Attr, 0, cloned.NumAttrs())
	cloned.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	redacted := h.redactAttrs(h.groups, attrs)
	out := slog.NewRecord(cloned.Time, cloned.Level, cloned.Message, cloned.PC)
	out.AddAttrs(redacted...)
	return out
}

func (h *filterHandler) redactAttrs(groups []string, attrs []slog.Attr) []slog.Attr {
	out := make([]slog.Attr, len(attrs))
	for i, a := range attrs {
		out[i] = h.redactAttr(groups, a)
	}
	return out
}

func (h *filterHandler) redactAttr(groups []string, a slog.Attr) slog.Attr {
	a.Value = a.Value.Resolve()
	if a.Value.Kind() == slog.KindGroup {
		group := a.Value.Group()
		next := make([]slog.Attr, len(group))
		nextGroups := appendPath(groups, a.Key)
		for i, ga := range group {
			next[i] = h.redactAttr(nextGroups, ga)
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(next...)}
	}
	if h.matchesKey(groups, a.Key) {
		return slog.Attr{Key: a.Key, Value: slog.StringValue(redactedValue)}
	}
	return a
}

func (h *filterHandler) matchesKey(groups []string, key string) bool {
	if _, ok := h.cfg.keys[key]; ok {
		return true
	}
	if len(groups) == 0 {
		return false
	}
	path := strings.Join(appendPath(groups, key), ".")
	_, ok := h.cfg.keys[path]
	return ok
}

func appendPath(groups []string, key string) []string {
	if key == "" {
		return groups
	}
	next := make([]string, 0, len(groups)+1)
	next = append(next, groups...)
	next = append(next, key)
	return next
}
