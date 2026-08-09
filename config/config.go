package config

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	// init encoding
	_ "github.com/go-kratos/kratos/v3/encoding/json"
	_ "github.com/go-kratos/kratos/v3/encoding/proto"
	_ "github.com/go-kratos/kratos/v3/encoding/xml"
	_ "github.com/go-kratos/kratos/v3/encoding/yaml"
	"github.com/go-kratos/kratos/v3/log"
)

/*
这是 Kratos 框架配置模块的核心实现，提供了一个多源加载、动态监听、类型安全访问的配置管理系统。下面逐层解析，重点展开 watch 方法。

整体架构概览

┌─────────────────────────────────────────────────────┐
│                    Config Interface                  │
│  Load() / Scan() / Value() / Watch() / Close()      │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────▼────────────┐
          │       config struct     │
          │  opts / reader / cached │
          │  observers / watchers   │
          └──┬──────────┬─────────┬─┘
             │          │         │
      Sources[]    Reader    sync.Map×2
      (Load+Watch) (Merge/   (cached +
                   Resolve/   observers)
                   Value/Source)

核心数据流：
Load：从多个 Source 拉取配置 → Merge → Resolve → 启动 Watch goroutine
Watch goroutine：持续监听变更 → Merge → Resolve → Diff → 通知 Observer
Value/Scan/Get：从缓存或 Reader 读取配置值

watch 方法深度解析

func (c *config) watch(w Watcher) {
    for {
        kvs, err := w.Next()
        // ... 错误处理 ...
        if err := c.reader.Merge(kvs...); err != nil { ... }
        if err := c.reader.Resolve(); err != nil { ... }
        c.cached.Range(func(key, value any) bool {
            // diff + notify
        })
    }
}

这是一个长生命周期的 goroutine

每个配置源（Source）在 Load() 时会启动一个独立的 watch goroutine：

// Load() 中
w, err := src.Watch()
c.watchers = append(c.watchers, w)
go c.watch(w)  // ← 每个 source 一个 goroutine

它永不主动退出（除非 context 被取消），是一个事件驱动的配置热更新循环。

四步工作流详解

Step 1: 阻塞等待变更

kvs, err := w.Next()

w.Next() 是阻塞调用，直到配置源检测到变更才返回新的 KV 列表
不同 Source 的实现不同：文件源用 fsnotify，etcd/consul 用 watch API，环境变量源可能永远不返回
返回的 kvs 是变更后的完整快照还是增量 diff，取决于具体 Watcher 实现

Step 2: 错误处理与重试

if errors.Is(err, context.Canceled) {
    log.Info("watcher's ctx cancel", "error", err)
    return  // ← 唯一正常退出路径
}
time.Sleep(time.Second)
log.Error("failed to watch next config", "error", err)
continue  // ← 非致命错误，1秒后重试

错误类型   行为
context.Canceled   优雅退出，goroutine 结束

其他错误   打日志 + sleep 1s + 继续循环（永不崩溃）

⚠️ 注意：这里没有指数退避，固定 1 秒重试。对于高频故障场景可能不够理想，但配置变更本身是低频事件，简单策略通常够用。

Step 3: 合并 + 解析

if err := c.reader.Merge(kvs...); err != nil { ... continue }
if err := c.reader.Resolve(); err != nil { ... continue }

这两步复用了 Load() 中的相同逻辑：

Merge：将新 KV 按优先级合并进 Reader 内部的 map[string]any 树（就是你之前看到的 defaultMerge）
Resolve：变量替换（如 ${DB_HOST} → 实际值）、格式转换等后处理

💡 Merge/Resolve 失败时只打日志并 continue，不会中断 watch 循环。这保证了即使某次变更格式有误，后续变更仍能被处理。

Step 4: Diff 检测 + Observer 通知（核心难点）

c.cached.Range(func(key, value any) bool {
    k := key.(string)
    v := value.(Value)
    if n, ok := c.reader.Value(k); ok &&
       reflect.TypeOf(n.Load()) == reflect.TypeOf(v.Load()) &&
       !reflect.DeepEqual(n.Load(), v.Load()) {
        v.Store(n.Load())
        if o, ok := c.observers.Load(k); ok {
            o.(Observer)(k, v)
        }
    }
    return true
})

这段代码做了三件事：

① 遍历所有已缓存的 key

c.cached.Range(...)

只检查曾经被访问过的 key（通过 Value() 方法懒加载进缓存）。未被任何代码读取过的配置项不参与 diff，避免无意义的反射开销。

② 三重条件判断是否变更

n, ok := c.reader.Value(k); ok &&                          // 新值存在
reflect.TypeOf(n.Load()) == reflect.TypeOf(v.Load()) &&    // 类型相同
!reflect.DeepEqual(n.Load(), v.Load())                      // 值不同

条件   目的
ok   新配置中该 key 被删除了 → 不通知（保留旧值语义）

类型相同   防止 int→float64 等隐式类型变化触发误报

DeepEqual   真正的值级比较，map/slice 内容变化也能检测到

⚠️ DeepEqual 的性能代价：对大 map/slice 做深比较是 O(n)。但由于只在配置变更时触发（极低频），且只遍历已缓存 key（通常很少），实际影响可忽略。

③ 原子更新 + 回调通知

v.Store(n.Load())           // 更新缓存中的 Value
o.(Observer)(k, v)          // 同步调用观察者回调

v.Store() 使后续 Value(k) 立即读到新值
Observer 是同步调用，如果回调耗时过长会阻塞整个 watch 循环
同一个 key 只能注册一个 Observer（observers.Store 会覆盖旧的）

watch 的设计哲学

"配置变更是低频事件，正确性 > 性能"

设计选择   权衡
每 source 独立 goroutine   隔离故障，一个源挂了不影响其他源

固定 1s 重试   简单可靠，避免退避复杂度

DeepEqual 全量 diff   精确检测变更，无需维护版本号/hash

同步 Observer 调用   保证通知顺序，但要求回调轻量

只 diff 已缓存 key   惰性求值，未使用的配置零开销

其他方法速览

Load()
Sources[].Load() → Merge → Watch() → go watch(w) → Resolve

一次性加载所有源，并为每个源启动 watch goroutine。

Value(key)
cached.Hit? → return
reader.Value(key)? → store to cached → return
→ return errValue{ErrNotFound}

懒缓存：首次访问时从 Reader 取值并缓存，后续直接读缓存。watch 更新时也写入缓存。

Scan(v)
将整个配置树反序列化为目标结构体（支持 proto.Message 和普通 struct，就是你之前问的 unmarshalJSON）。

GetT
泛型便捷函数，自动根据 T 的类型选择 Bool/Int/Float/String/Scan：
port, _ := config.Getint
name, _ := config.Getstring
db, _ := config.GetDatabaseConfig  // fallback to Scan

Watch(key, observer)
注册 key 级别的变更回调。前置校验：key 必须已存在于当前配置中（防止拼写错误静默失败）。

⚠️ 潜在问题与改进方向
问题   说明
Observer 同步调用   慢回调阻塞 watch 循环，应改为异步 dispatch

无退避重试   持久故障时每秒打一条 error 日志，建议指数退避+上限

单 Observer/key   同一 key 只能一个观察者，可用 slice 或 fan-out 支持多订阅

key 删除不通知   配置项被移除时 Observer 收不到事件，可能需要 tombstone 语义

DeepEqual 对 NaN   float64(NaN) != float64(NaN)，含 NaN 的配置可能每次都触发通知

竞态窗口   Merge→Resolve→Diff 之间无锁，并发 Load/Watch 可能读到中间状态

一句话总结

watch 是一个永不停歇的配置热更新协程：阻塞等待变更 → 容错重试 → 合并解析 → 对已缓存 key 做类型安全的 DeepEqual diff → 同步通知 Observer。它以"低频事件的正确性优先"为设计原则，用简洁的同步模型实现了配置的多源叠加、动态感知和按需通知。

*/

var _ Config = (*config)(nil)

var ErrNotFound = errors.New("key not found") // ErrNotFound is key not found.

// Observer is config observer.
type Observer func(string, Value)

// Config is a config interface.
type Config interface {
	Load() error
	Scan(v any) error
	Value(key string) Value
	Watch(key string, o Observer) error
	Close() error
}

type config struct {
	opts      options
	reader    Reader
	cached    sync.Map
	observers sync.Map
	watchers  []Watcher
}

// New a config with options.
func New(opts ...Option) Config {
	o := options{
		decoder:  defaultDecoder,
		resolver: defaultResolver,
		merge:    defaultMerge,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &config{
		opts:   o,
		reader: newReader(o),
	}
}

func (c *config) watch(w Watcher) {
	for {
		kvs, err := w.Next()
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("watcher's ctx cancel", "error", err)
				return
			}
			time.Sleep(time.Second)
			log.Error("failed to watch next config", "error", err)
			continue
		}
		if err := c.reader.Merge(kvs...); err != nil {
			log.Error("failed to merge next config", "error", err)
			continue
		}
		if err := c.reader.Resolve(); err != nil {
			log.Error("failed to resolve next config", "error", err)
			continue
		}
		c.cached.Range(func(key, value any) bool {
			k := key.(string)
			v := value.(Value)
			if n, ok := c.reader.Value(k); ok && reflect.TypeOf(n.Load()) == reflect.TypeOf(v.Load()) && !reflect.DeepEqual(n.Load(), v.Load()) {
				v.Store(n.Load())
				if o, ok := c.observers.Load(k); ok {
					o.(Observer)(k, v)
				}
			}
			return true
		})
	}
}

func (c *config) Load() error {
	for _, src := range c.opts.sources {
		kvs, err := src.Load()
		if err != nil {
			return err
		}
		for _, v := range kvs {
			log.Debug("config loaded", "key", v.Key, "format", v.Format)
		}
		if err = c.reader.Merge(kvs...); err != nil {
			log.Error("failed to merge config source", "error", err)
			return err
		}
		w, err := src.Watch()
		if err != nil {
			log.Error("failed to watch config source", "error", err)
			return err
		}
		c.watchers = append(c.watchers, w)
		go c.watch(w)
	}
	if err := c.reader.Resolve(); err != nil {
		log.Error("failed to resolve config source", "error", err)
		return err
	}
	return nil
}

func (c *config) Value(key string) Value {
	if v, ok := c.cached.Load(key); ok {
		return v.(Value)
	}
	if v, ok := c.reader.Value(key); ok {
		c.cached.Store(key, v)
		return v
	}
	return &errValue{err: ErrNotFound}
}

func (c *config) Scan(v any) error {
	data, err := c.reader.Source()
	if err != nil {
		return err
	}
	return unmarshalJSON(data, v)
}

func (c *config) Watch(key string, o Observer) error {
	if v := c.Value(key); v.Load() == nil {
		return ErrNotFound
	}
	c.observers.Store(key, o)
	return nil
}

func (c *config) Close() error {
	for _, w := range c.watchers {
		if err := w.Stop(); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a config value by key and scans it into the target type.
func Get[T any](c Config, key string) (T, error) {
	var t T
	v := c.Value(key)

	if v.Load() == nil {
		return t, ErrNotFound
	}

	switch any(t).(type) {
	case bool:
		b, err := v.Bool()
		return any(b).(T), err
	case int64:
		i, err := v.Int()
		return any(i).(T), err
	case int:
		i, err := v.Int()
		return any(int(i)).(T), err
	case float64:
		f, err := v.Float()
		return any(f).(T), err
	case string:
		s, err := v.String()
		return any(s).(T), err
	}

	err := v.Scan(&t)
	return t, err
}
