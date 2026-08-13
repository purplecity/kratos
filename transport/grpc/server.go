package grpc

import (
	"context"
	"crypto/tls"
	"net"
	"net/url"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/admin"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/go-kratos/kratos/v3/internal/endpoint"
	"github.com/go-kratos/kratos/v3/internal/host"
	"github.com/go-kratos/kratos/v3/internal/matcher"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
)

/*
是的，这三个都是 gRPC 官方（google.golang.org/grpc）提供的标准扩展包。它们不属于 gRPC 核心协议，但属于生产级 gRPC 服务的必备基础设施。Kratos 默认集成它们，是为了让服务“开箱即生产”。

以下是三者的具体职责和区别：

Health Check (grpc/health)
是什么： 实现了 grpc.health.v1.Health 这个标准化的 protobuf 服务定义。
干嘛用： 让外部系统（K8s、负载均衡器、服务网格）能以统一协议探测服务是否存活、就绪。
为什么重要： 如果没有它，每个服务的健康检查接口都不一样（有的用 HTTP /healthz，有的自定义 RPC），运维无法统一管理。gRPC 官方定义了这个标准后，所有生态组件都认它。
Kratos 中的行为：
    默认自动注册，无需手动配置
    Start() 时调用 Resume() 标记为 SERVING
    Stop() 时调用 Shutdown() 标记为 NOT_SERVING（优雅下线期间告诉 LB 别再发新请求了）
    可通过 CustomHealth() Option 禁用默认实现，替换为自己的逻辑

Reflection (grpc/reflection)
是什么： 实现了 gRPC Server Reflection Protocol，允许客户端在运行时动态查询服务端暴露了哪些 Service、Method、Message 类型及其 proto 定义。
干嘛用：
    调试神器： grpcurl、grpcui、Postman 等工具依赖它来自动发现接口并构造请求，无需提前拿到 .proto 文件
    代码生成： 某些动态客户端框架通过反射获取服务描述符来生成调用桩
    服务网格： Envoy/Istio 可能利用反射进行路由配置验证
安全注意： 生产环境通常建议关闭（DisableReflection()），因为它会暴露完整的 API 结构，可能被攻击者用于侦察。
Kratos 中的行为： 默认开启，提供 DisableReflection() Option 显式关闭。

Admin (grpc/admin)
是什么： gRPC 较新版本引入的标准化管理服务（google.golang.org/grpc/admin），提供对服务器内部状态的运行时查询与管理能力。
干嘛用： 与 Health/Reflection 不同，Admin 关注的是运维管理面而非业务数据面。典型功能包括：
    查询当前连接数、活跃流数量
    查看/修改 Channelz 诊断信息
    运行时调整日志级别、限流阈值等配置
    触发内存 dump、GC 等诊断操作
与 Health 的区别： Health 只回答“活没活着”，Admin 回答“活得怎么样、能不能在线调参”。
Kratos 中的行为：
    通过 admin.Register(srv.Server) 注册
    返回一个 clean func，在 Stop() 时调用以清理资源
    这是三者中最新、最不常用的一个，很多项目实际上不会直接用到

三者对比总结
维度   Health   Reflection   Admin
服务对象   K8s / LB / Mesh   开发者 / 调试工具   运维 / SRE

核心问题   “能接请求吗？”   “有哪些接口？”   “内部状态如何？能在线调吗？”

协议标准   grpc.health.v1   Server Reflection Protocol   grpc.admin.v1 (较新)

生产环境   ✅ 必须开   ⚠️ 建议关   🔧 按需开

Kratos 默认   自动注册   自动注册   自动注册

关闭方式   CustomHealth()   DisableReflection()   无内置 Option，需自行处理

💡 设计哲学

Kratos 把这三个都默认打开，体现了一个核心理念：框架应该保证服务注册到 K8s/Mesh 后立刻可被正确治理，而不是让每个业务开发者自己去踩坑补全这些基础设施。 如果你不需要某个能力，显式关闭即可；但如果你忘了加 Health，上线后 K8s 探针失败导致 Pod 反复重启，那就是事故了。默认安全 > 默认精简。
*/

var (
	_ transport.Server     = (*Server)(nil)
	_ transport.Endpointer = (*Server)(nil)
)

// ServerOption is gRPC server option.
type ServerOption func(o *Server)

// Network with server network.
func Network(network string) ServerOption {
	return func(s *Server) {
		s.network = network
	}
}

// Address with server address.
func Address(addr string) ServerOption {
	return func(s *Server) {
		s.address = addr
	}
}

// Endpoint with server address.
func Endpoint(endpoint *url.URL) ServerOption {
	return func(s *Server) {
		s.endpoint = endpoint
	}
}

// Timeout with server timeout.
func Timeout(timeout time.Duration) ServerOption {
	return func(s *Server) {
		s.timeout = timeout
	}
}

// Middleware with server middleware.
func Middleware(m ...middleware.Middleware) ServerOption {
	return func(s *Server) {
		s.middleware.Use(m...)
	}
}

func StreamMiddleware(m ...middleware.Middleware) ServerOption {
	return func(s *Server) {
		s.streamMiddleware.Use(m...)
	}
}

// CustomHealth Checks server.
func CustomHealth() ServerOption {
	return func(s *Server) {
		s.customHealth = true
	}
}

// TLSConfig with TLS config.
func TLSConfig(c *tls.Config) ServerOption {
	return func(s *Server) {
		s.tlsConf = c
	}
}

// Listener with server lis
func Listener(lis net.Listener) ServerOption {
	return func(s *Server) {
		s.lis = lis
	}
}

// UnaryInterceptor returns a ServerOption that sets the UnaryServerInterceptor for the server.
func UnaryInterceptor(in ...grpc.UnaryServerInterceptor) ServerOption {
	return func(s *Server) {
		s.unaryInts = in
	}
}

// StreamInterceptor returns a ServerOption that sets the StreamServerInterceptor for the server.
func StreamInterceptor(in ...grpc.StreamServerInterceptor) ServerOption {
	return func(s *Server) {
		s.streamInts = in
	}
}

// DisableReflection disable grpc reflection.
func DisableReflection() ServerOption {
	return func(s *Server) {
		s.disableReflection = true
	}
}

// Options with grpc options.
func Options(opts ...grpc.ServerOption) ServerOption {
	return func(s *Server) {
		s.grpcOpts = opts
	}
}

/*
这两个字段是 Kratos gRPC Server 对原生 gRPC 拦截器（Interceptor）机制的有序缓存。它们的存在是为了解决一个核心矛盾：Kratos 的 Middleware 抽象与 gRPC 原生 Interceptor 机制之间的适配问题。

它们是什么？
字段   类型   作用时机   对应 gRPC 原生概念
unaryInts   []grpc.UnaryServerInterceptor   普通一元 RPC 调用（请求-响应）   grpc.UnaryInterceptor()

streamInts   []grpc.StreamServerInterceptor   流式 RPC 调用（客户端流/服务端流/双向流）   grpc.StreamInterceptor()

⚠️ 关键认知
这两个切片里存的不是 Kratos 的 middleware.Middleware，而是已经转换完成的、标准的 gRPC 原生拦截器函数。

为什么不直接用 gRPC 原生的 Option？

gRPC 原生有一个致命限制：
// ❌ gRPC 原生只允许设置 ONE 个 UnaryInterceptor
grpc.NewServer(grpc.UnaryInterceptor(myInterceptor))

如果你调用两次 grpc.UnaryInterceptor()，后一次会直接覆盖前一次。而微服务框架通常需要叠加多个拦截器（日志 → 鉴权 → 限流 → 链路追踪 → 业务逻辑）。

虽然 gRPC 官方后来提供了 grpc.ChainUnaryInterceptor()，但 Kratos 需要在构建 Server 的过程中动态地、按顺序地组装拦截器链，而不是在创建 grpc.Server 时一次性传入。因此 Kratos 选择了：

先用 unaryInts / streamInts 切片暂存所有拦截器
在最终启动时，统一用 grpc.ChainUnaryInterceptor(unaryInts...) 和 grpc.ChainStreamInterceptor(streamInts...) 注入

Kratos Middleware 是如何变成 Interceptor 的？

这是理解这两个字段的核心。Kratos 的业务开发者写的是统一的 Middleware：

// 用户写的 Kratos middleware，与传输协议无关
func LoggingMiddleware() middleware.Middleware {
    return func(handler middleware.Handler) middleware.Handler {
        return func(ctx context.Context, req interface{}) (interface{}, error) {
            log.Info("request received")
            return handler(ctx, req)
        }
    }
}

但 gRPC 引擎不认识这个签名。Kratos 内部做了一个适配器转换：

Kratos Middleware (ctx, req) → (resp, err)
        ↓ 适配层
gRPC UnaryServerInterceptor (ctx, req, info, handler) → (resp, err)
        ↓ 存入
unaryInts []grpc.UnaryServerInterceptor

这个适配层做了三件关键事：
将 gRPC 的 ServerTransportStream 信息注入 context
将 Kratos 的 transport.ServerContext 元数据填入 ctx
把 gRPC 的 handler(ctx, req) 包装成 Kratos 的 middleware.Handler

执行顺序与分层

最终注入 gRPC 时的拦截器链是有严格顺序的：

请求进入
  │
  ├─ 1. 用户通过 grpc.WithUnaryInterceptor() 传入的原生拦截器
  │     （直接追加到 unaryInts 末尾，用于兼容纯 gRPC 生态组件）
  │
  ├─ 2. Kratos Middleware 转换而来的拦截器
  │     （按 srv.Use() 的注册顺序排列）
  │
  └─ 3. Kratos 内置拦截器（如 codec 适配、错误码转换）
        （始终在最内层，紧贴业务 handler）
  │
  ▼
实际业务方法执行

为什么 Unary 和 Stream 要分开？

这不是 Kratos 的设计选择，而是 gRPC 协议本身的约束：

Unary 拦截器接收 (ctx, req)，返回 (resp, err) —— 请求和响应都是单次值
Stream 拦截器接收 (srv, ss)，其中 ss 是一个流对象，需要手动 Recv/Send —— 生命周期完全不同

两者签名不兼容，无法用同一个函数处理。所以 Kratos 必须维护两个独立的切片，分别适配、分别注入。如果你的 Middleware 同时需要支持 Unary 和 Stream，Kratos 会在适配层自动生成两个版本的拦截器函数，分别放入对应的切片中。

总结

unaryInts 和 streamInts 本质上是 Kratos 统一中间件抽象与 gRPC 原生拦截器协议之间的翻译缓冲区。它们让业务开发者只需写一套协议无关的 Middleware，同时保留了直接注入原生 gRPC 拦截器的逃生通道，并在最终启动时将两者有序合并为 gRPC 可执行的拦截器链。
*/

// Server is a gRPC server wrapper.
type Server struct {
	*grpc.Server
	baseCtx           context.Context
	tlsConf           *tls.Config
	lis               net.Listener
	err               error
	network           string
	address           string
	endpoint          *url.URL
	timeout           time.Duration
	middleware        matcher.Matcher
	streamMiddleware  matcher.Matcher
	unaryInts         []grpc.UnaryServerInterceptor
	streamInts        []grpc.StreamServerInterceptor
	grpcOpts          []grpc.ServerOption
	health            *health.Server
	customHealth      bool
	adminClean        func()
	disableReflection bool
}

// NewServer creates a gRPC server by options.
func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		baseCtx:          context.Background(),
		network:          "tcp",
		address:          ":0",
		timeout:          1 * time.Second,
		health:           health.NewServer(),
		middleware:       matcher.New(),
		streamMiddleware: matcher.New(),
	}
	for _, o := range opts {
		o(srv)
	}
	unaryInts := []grpc.UnaryServerInterceptor{
		srv.unaryServerInterceptor(),
	}
	streamInts := []grpc.StreamServerInterceptor{
		srv.streamServerInterceptor(),
	}
	if len(srv.unaryInts) > 0 {
		unaryInts = append(unaryInts, srv.unaryInts...)
	}
	if len(srv.streamInts) > 0 {
		streamInts = append(streamInts, srv.streamInts...)
	}
	grpcOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryInts...),
		grpc.ChainStreamInterceptor(streamInts...),
	}
	if srv.tlsConf != nil {
		grpcOpts = append(grpcOpts, grpc.Creds(credentials.NewTLS(srv.tlsConf)))
	}
	if len(srv.grpcOpts) > 0 {
		grpcOpts = append(grpcOpts, srv.grpcOpts...)
	}
	srv.Server = grpc.NewServer(grpcOpts...)
	// internal register
	if !srv.customHealth {
		grpc_health_v1.RegisterHealthServer(srv.Server, srv.health)
	}
	// reflection register
	if !srv.disableReflection {
		reflection.Register(srv.Server)
	}
	// admin register
	srv.adminClean, _ = admin.Register(srv.Server)
	return srv
}

// Use uses a service middleware with selector.
// selector:
//   - '/*'
//   - '/helloworld.v1.Greeter/*'
//   - '/helloworld.v1.Greeter/SayHello'
func (s *Server) Use(selector string, m ...middleware.Middleware) {
	s.middleware.Add(selector, m...)
}

// Endpoint return a real address to registry endpoint.
// examples:
//
//	grpc://127.0.0.1:9000?isSecure=false
func (s *Server) Endpoint() (*url.URL, error) {
	if err := s.listenAndEndpoint(); err != nil {
		return nil, s.err
	}
	return s.endpoint, nil
}

// Start start the gRPC server.
func (s *Server) Start(ctx context.Context) error {
	if err := s.listenAndEndpoint(); err != nil {
		return s.err
	}
	s.baseCtx = ctx
	log.Info("[gRPC] server listening", "addr", s.lis.Addr().String())
	s.health.Resume()
	return s.Serve(s.lis)
}

// Stop stop the gRPC server.
func (s *Server) Stop(ctx context.Context) error {
	if s.adminClean != nil {
		s.adminClean()
	}
	s.health.Shutdown()

	done := make(chan struct{})
	go func() {
		defer close(done)
		log.Info("[gRPC] server stopping")
		s.GracefulStop()
	}()

	select {
	case <-done:
	case <-ctx.Done():
		log.Warn("[gRPC] server couldn't stop gracefully in time, doing force stop")
		s.Server.Stop()
	}
	return nil
}

func (s *Server) listenAndEndpoint() error {
	if s.lis == nil {
		lis, err := net.Listen(s.network, s.address)
		if err != nil {
			s.err = err
			return err
		}
		s.lis = lis
	}
	if s.endpoint == nil {
		addr, err := host.Extract(s.address, s.lis)
		if err != nil {
			s.err = err
			return err
		}
		s.endpoint = endpoint.NewEndpoint(endpoint.Scheme("grpc", s.tlsConf != nil), addr)
	}
	return s.err
}
