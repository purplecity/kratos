package kratos

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/transport"
)

/*
这段代码是 Kratos v3 框架的核心生命周期管理逻辑。Run() 方法设计得非常精巧，它利用 errgroup 实现了多服务并发启动、优雅停止和信号监听的编排。

下面我先解释 errgroup，再结合代码详细拆解 Run() 的执行流程。

一、先搞懂 errgroup

golang.org/x/sync/errgroup 是 Go 官方扩展库，你可以把它理解为 “带错误传播和上下文取消能力的 WaitGroup”。

与 sync.WaitGroup 的核心区别
特性   sync.WaitGroup   errgroup.Group
等待所有 goroutine 完成   ✅   ✅

任意 goroutine 返回 error 时自动取消 context   ❌   ✅

获取第一个非 nil 的 error   ❌（需自己加 channel/mutex）   ✅ (Wait() 返回值)

限制最大并发数   ❌   ✅ (SetLimit(n))

核心 API

// 创建带 context 的 errgroup
// 当任意 Go() 中的函数返回非 nil error，或外部 ctx 被取消时，
// 返回的 ctx 会被自动 cancel
eg, ctx := errgroup.WithContext(parentCtx)

// 启动一个 goroutine
eg.Go(func() error {
    // 如果这里 return err != nil，ctx 会被 cancel
    return nil
})

// 阻塞等待所有 goroutine 完成，返回第一个非 nil error
err := eg.Wait()

为什么 Kratos 要用它？

在微服务应用中，一个 App 可能同时运行 HTTP Server、gRPC Server、定时任务等多个组件。我们需要：
并发启动所有 server
任意 server 启动失败时，其他 server 也要停下来（错误传播）
收到 SIGTERM 信号时，所有 server 都要优雅退出（context 取消）

这三点正好是 errgroup.WithContext 的原生能力，用原生 WaitGroup 实现会非常啰嗦且容易出错。

二、Run() 方法逐段详解

我把 Run() 按执行阶段拆成 6 步：

Step 1: 准备阶段
instance, err := a.buildInstance()  // 构建服务实例信息（ID/Name/Endpoints等）
a.mu.Lock()
a.instance = instance               // 线程安全地存储实例
a.mu.Unlock()

sctx := NewContext(a.ctx, a)        // 将 AppInfo 注入 context
eg, ctx := errgroup.WithContext(sctx) // ⭐ 创建 errgroup，ctx 会在任一 goroutine 报错时被 cancel
wg := sync.WaitGroup{}              // ⭐ 额外的 WaitGroup，用途见下文

⚠️ 关键细节：这里同时创建了 eg 和 wg，它们各司其职：
eg：管理所有长期运行的 goroutine + 错误传播
wg：仅用于确保 server.Start() 已经被调用后，才去注册服务

Step 2: BeforeStart 钩子
for _, fn := range a.opts.beforeStart {
    if err = fn(sctx); err != nil {
        return err  // 前置钩子失败，直接退出，不启动任何服务
    }
}

Step 3: 启动所有 Server（最精妙的部分）
octx := NewContext(a.opts.ctx, a)  // ⭐ 注意：这里用的是原始 ctx，不是 errgroup 的 ctx

for _, srv := range a.opts.servers {
    server := srv  // 闭包捕获，避免循环变量问题

    // Goroutine A: 等待停止信号，然后优雅关闭 server
    eg.Go(func() error {
        <-ctx.Done()  // 阻塞直到 errgroup ctx 被取消（报错或收到信号）
        stopCtx := context.WithoutCancel(octx) // Go 1.21+，创建不受父 ctx 取消影响的 context
        if a.opts.stopTimeout > 0 {
            var cancel context.CancelFunc
            stopCtx, cancel = context.WithTimeout(stopCtx, a.opts.stopTimeout)
            defer cancel()
        }
        return server.Stop(stopCtx)
    })

    // Goroutine B: 启动 server
    wg.Add(1)
    eg.Go(func() error {
        wg.Done()  // ⭐ 标记 Start() 已开始执行
        return server.Start(octx) // Start 通常是阻塞的，直到服务关闭才返回
    })
}

wg.Wait()  // ⭐ 阻塞直到所有 server.Start() 都已被调用

🔑 为什么需要额外的 wg？

这是一个时序保证问题：

没有 wg 的情况（有 bug）：
  Goroutine B (Start) 还没被调度执行
  → 主协程已经走到 registrar.Register()
  → 注册中心里有了这个服务，但 server 实际还没开始监听端口
  → 流量打过来 → 连接拒绝 ❌

有 wg 的情况（正确）：
  wg.Add(1) 在 eg.Go 之前
  wg.Done() 在 server.Start() 的第一行
  wg.Wait() 确保所有 Start() 至少已经开始执行
  → 此时 server 已经在 listen 了
  → 再去注册中心注册 ✅

🔑 为什么 Stop 用 <-ctx.Done() 而不是直接在 Start 返回后调用？

因为 server.Start(octx) 使用的是原始 context（不受 errgroup 控制），它只会在自身正常关闭时才返回。而停止信号需要通过 errgroup 的 ctx 来传递。所以单独开一个 goroutine 监听 ctx.Done() 来触发 Stop。

🔑 context.WithoutCancel 的作用（Go 1.21+）

当 errgroup 的 ctx 因错误被 cancel 时，Stop 仍然需要一个干净的 context 来执行清理操作（如刷盘、断开连接）。WithoutCancel 创建了一个继承父 context 值但不继承取消信号的 context，确保 Stop 不会被意外中断。

Step 4: 注册服务 + AfterStart 钩子
// wg.Wait() 之后，所有 server 已在监听，可以安全注册了
if a.opts.registrar != nil {
    rctx, rcancel := context.WithTimeout(ctx, a.opts.registrarTimeout)
    defer rcancel()
    if err = a.opts.registrar.Register(rctx, instance); err != nil {
        return err  // 注册失败 → errgroup ctx 被 cancel → 所有 server 触发 Stop
    }
}

for _, fn := range a.opts.afterStart {
    if err = fn(sctx); err != nil {
        return err  // 后置钩子失败同样触发全局停止
    }
}

Step 5: 监听系统信号
c := make(chan os.Signal, 1)
signal.Notify(c, a.opts.sigs...)  // SIGTERM, SIGQUIT, SIGINT

eg.Go(func() error {
    select {
    case <-ctx.Done():
        return nil       // 已经是错误导致的退出，不重复处理
    case <-c:
        return a.Stop()  // ⭐ 收到信号 → 调用 Stop() → cancel ctx → 触发所有 server.Stop()
    }
})

Step 6: 等待退出 + AfterStop 钩子
// 阻塞直到所有 goroutine 完成
if err = eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
    return err
}

// 执行清理钩子
err = nil
for _, fn := range a.opts.afterStop {
    err = fn(sctx)
}
return err

三、整体流程图

Run()
 │
 ├── beforeStart hooks
 │
 ├── for each server:
 │     ├── eg.Go(←ctx.Done→ server.Stop())   [等待停止]
 │     └── eg.Go(server.Start())             [启动服务]
 │         └── wg.Done()
 ├── wg.Wait() ← 确保所有 Start() 已调用
 │
 ├── registrar.Register()                    [安全注册]
 ├── afterStart hooks
 │
 ├── eg.Go(signal.Notify → a.Stop())         [监听信号]
 │
 ├── eg.Wait() ← 阻塞直到以下任一发生:
 │     ├── 某个 server.Start() 返回 error
 │     ├── 某个 hook 返回 error
 │     ├── 收到 SIGTERM/SIGINT → a.Stop()
 │     │     ├── beforeStop hooks
 │     │     ├── registrar.Deregister()
 │     │     └── a.cancel() → ctx.Done() → 所有 server.Stop()
 │     └── 外部 context 被取消
 │
 └── afterStop hooks

四、学习建议

如果你想深入掌握这段代码的设计思想，建议做以下练习：

手写简化版：去掉 hooks 和 registry，只用 errgroup 实现“并发启动两个 HTTP server + 信号优雅退出”，体会 wg 的必要性
故意制造竞态：去掉 wg.Wait()，在注册后立即发请求，验证是否会出现连接拒绝
阅读 errgroup 源码：只有约 80 行，理解 WithContext 内部如何用 sync.Once 保证只 cancel 一次、只记录第一个 error

这段代码是 Go 语言中并发编排的典范，吃透它对理解整个 Kratos 框架乃至 Go 微服务架构都有很大帮助。
*/

// AppInfo is application context value.
type AppInfo interface {
	ID() string
	Name() string
	Version() string
	Metadata() map[string]string
	Endpoint() []string
}

// App is an application components lifecycle manager.
type App struct {
	opts     options
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	instance *registry.ServiceInstance
}

// New create an application lifecycle manager.
func New(opts ...Option) *App {
	o := options{
		ctx:              context.Background(),
		sigs:             []os.Signal{syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT},
		registrarTimeout: 10 * time.Second,
	}
	if id, err := uuid.NewUUID(); err == nil {
		o.id = id.String()
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.logger != nil {
		log.SetDefault(o.logger)
	}
	ctx, cancel := context.WithCancel(o.ctx)
	return &App{
		ctx:    ctx,
		cancel: cancel,
		opts:   o,
	}
}

// ID returns app instance id.
func (a *App) ID() string { return a.opts.id }

// Name returns service name.
func (a *App) Name() string { return a.opts.name }

// Version returns app version.
func (a *App) Version() string { return a.opts.version }

// Metadata returns service metadata.
func (a *App) Metadata() map[string]string { return a.opts.metadata }

// Endpoint returns endpoints.
func (a *App) Endpoint() []string {
	if a.instance != nil {
		return a.instance.Endpoints
	}
	return nil
}

// Run executes all OnStart hooks registered with the application's Lifecycle.
func (a *App) Run() error {
	instance, err := a.buildInstance()
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.instance = instance
	a.mu.Unlock()
	sctx := NewContext(a.ctx, a)
	eg, ctx := errgroup.WithContext(sctx)
	wg := sync.WaitGroup{}

	for _, fn := range a.opts.beforeStart {
		if err = fn(sctx); err != nil {
			return err
		}
	}
	octx := NewContext(a.opts.ctx, a)
	for _, srv := range a.opts.servers {
		server := srv
		eg.Go(func() error {
			<-ctx.Done() // wait for stop signal
			stopCtx := context.WithoutCancel(octx)
			if a.opts.stopTimeout > 0 {
				var cancel context.CancelFunc
				stopCtx, cancel = context.WithTimeout(stopCtx, a.opts.stopTimeout)
				defer cancel()
			}
			return server.Stop(stopCtx)
		})
		wg.Add(1)
		eg.Go(func() error {
			wg.Done() // here is to ensure server start has begun running before register, so defer is not needed
			return server.Start(octx)
		})
	}
	wg.Wait()
	if a.opts.registrar != nil {
		rctx, rcancel := context.WithTimeout(ctx, a.opts.registrarTimeout)
		defer rcancel()
		if err = a.opts.registrar.Register(rctx, instance); err != nil {
			return err
		}
	}
	for _, fn := range a.opts.afterStart {
		if err = fn(sctx); err != nil {
			return err
		}
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, a.opts.sigs...)
	eg.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case <-c:
			return a.Stop()
		}
	})
	if err = eg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	err = nil
	for _, fn := range a.opts.afterStop {
		err = fn(sctx)
	}
	return err
}

// Stop gracefully stops the application.
func (a *App) Stop() (err error) {
	sctx := NewContext(a.ctx, a)
	for _, fn := range a.opts.beforeStop {
		err = fn(sctx)
	}

	a.mu.Lock()
	instance := a.instance
	a.mu.Unlock()
	if a.opts.registrar != nil && instance != nil {
		ctx, cancel := context.WithTimeout(NewContext(a.ctx, a), a.opts.registrarTimeout)
		defer cancel()
		if err = a.opts.registrar.Deregister(ctx, instance); err != nil {
			return err
		}
	}
	if a.cancel != nil {
		a.cancel()
	}
	return err
}

func (a *App) buildInstance() (*registry.ServiceInstance, error) {
	endpoints := make([]string, 0, len(a.opts.endpoints))
	for _, e := range a.opts.endpoints {
		endpoints = append(endpoints, e.String())
	}
	if len(endpoints) == 0 {
		for _, srv := range a.opts.servers {
			if r, ok := srv.(transport.Endpointer); ok {
				e, err := r.Endpoint()
				if err != nil {
					return nil, err
				}
				endpoints = append(endpoints, e.String())
			}
		}
	}
	return &registry.ServiceInstance{
		ID:        a.opts.id,
		Name:      a.opts.name,
		Version:   a.opts.version,
		Metadata:  a.opts.metadata,
		Endpoints: endpoints,
	}, nil
}

type appKey struct{}

// NewContext returns a new Context that carries value.
func NewContext(ctx context.Context, s AppInfo) context.Context {
	return context.WithValue(ctx, appKey{}, s)
}

// FromContext returns the Transport value stored in ctx, if any.
func FromContext(ctx context.Context) (s AppInfo, ok bool) {
	s, ok = ctx.Value(appKey{}).(AppInfo)
	return
}
