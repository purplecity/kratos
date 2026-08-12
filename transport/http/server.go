package http

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/mux"

	"github.com/go-kratos/kratos/v3/internal/endpoint"
	"github.com/go-kratos/kratos/v3/internal/host"
	"github.com/go-kratos/kratos/v3/internal/matcher"
	"github.com/go-kratos/kratos/v3/log"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/transport"
)

var (
	_ transport.Server     = (*Server)(nil)
	_ transport.Endpointer = (*Server)(nil)
	_ http.Handler         = (*Server)(nil)
)

// ServerOption is an HTTP server option.
type ServerOption func(*Server)

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

// Middleware with service middleware option.
func Middleware(m ...middleware.Middleware) ServerOption {
	return func(o *Server) {
		o.middleware.Use(m...)
	}
}

// Filter with HTTP middleware option.
func Filter(filters ...FilterFunc) ServerOption {
	return func(o *Server) {
		o.filters = filters
	}
}

// RequestVarsDecoder with request decoder.
func RequestVarsDecoder(dec DecodeRequestFunc) ServerOption {
	return func(o *Server) {
		o.decVars = dec
	}
}

// RequestQueryDecoder with request decoder.
func RequestQueryDecoder(dec DecodeRequestFunc) ServerOption {
	return func(o *Server) {
		o.decQuery = dec
	}
}

// RequestDecoder with request decoder.
func RequestDecoder(dec DecodeRequestFunc) ServerOption {
	return func(o *Server) {
		o.decBody = dec
	}
}

// ResponseEncoder with response encoder.
func ResponseEncoder(en EncodeResponseFunc) ServerOption {
	return func(o *Server) {
		o.enc = en
	}
}

// ErrorEncoder with error encoder.
func ErrorEncoder(en EncodeErrorFunc) ServerOption {
	return func(o *Server) {
		o.ene = en
	}
}

// TLSConfig with TLS config.
func TLSConfig(c *tls.Config) ServerOption {
	return func(o *Server) {
		o.tlsConf = c
	}
}

// StrictSlash is with mux's StrictSlash
// If true, when the path pattern is "/path/", accessing "/path" will
// redirect to the former and vice versa.
func StrictSlash(strictSlash bool) ServerOption {
	return func(o *Server) {
		o.strictSlash = strictSlash
	}
}

// Listener with server lis
func Listener(lis net.Listener) ServerOption {
	return func(s *Server) {
		s.lis = lis
	}
}

// PathPrefix with mux's PathPrefix, router will be replaced by a subrouter that start with prefix.
func PathPrefix(prefix string) ServerOption {
	return func(s *Server) {
		s.router = s.router.PathPrefix(prefix).Subrouter()
	}
}

func NotFoundHandler(handler http.Handler) ServerOption {
	return func(s *Server) {
		s.router.NotFoundHandler = handler
	}
}

func MethodNotAllowedHandler(handler http.Handler) ServerOption {
	return func(s *Server) {
		s.router.MethodNotAllowedHandler = handler
	}
}

// Server is an HTTP server wrapper.
type Server struct {
	*http.Server
	lis         net.Listener
	tlsConf     *tls.Config
	endpoint    *url.URL
	err         error
	network     string
	address     string
	timeout     time.Duration
	filters     []FilterFunc
	middleware  matcher.Matcher
	decVars     DecodeRequestFunc
	decQuery    DecodeRequestFunc
	decBody     DecodeRequestFunc
	enc         EncodeResponseFunc
	ene         EncodeErrorFunc
	strictSlash bool
	router      *mux.Router
}

// NewServer creates an HTTP server by options.
func NewServer(opts ...ServerOption) *Server {
	srv := &Server{
		network:     "tcp",
		address:     ":0",
		timeout:     1 * time.Second,
		middleware:  matcher.New(),
		decVars:     DefaultRequestVars,
		decQuery:    DefaultRequestQuery,
		decBody:     DefaultRequestDecoder,
		enc:         DefaultResponseEncoder,
		ene:         DefaultErrorEncoder,
		strictSlash: true,
		router:      mux.NewRouter(),
	}
	srv.router.NotFoundHandler = http.DefaultServeMux
	srv.router.MethodNotAllowedHandler = http.DefaultServeMux
	for _, o := range opts {
		o(srv)
	}
	srv.router.StrictSlash(srv.strictSlash)
	srv.router.Use(srv.filter())
	srv.Server = &http.Server{
		Handler:   FilterChain(srv.filters...)(srv.router),
		TLSConfig: srv.tlsConf,
	}
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

// WalkRoute walks the router and all its sub-routers, calling walkFn for each route in the tree.
func (s *Server) WalkRoute(fn WalkRouteFunc) error {
	return s.router.Walk(func(route *mux.Route, _ *mux.Router, _ []*mux.Route) error {
		methods, err := route.GetMethods()
		if err != nil {
			return nil // ignore no methods
		}
		path, err := route.GetPathTemplate()
		if err != nil {
			return err
		}
		for _, method := range methods {
			if err := fn(RouteInfo{Method: method, Path: path}); err != nil {
				return err
			}
		}
		return nil
	})
}

// WalkHandle walks the router and all its sub-routers, calling walkFn for each route in the tree.
func (s *Server) WalkHandle(handle func(method, path string, handler http.HandlerFunc)) error {
	return s.WalkRoute(func(r RouteInfo) error {
		handle(r.Method, r.Path, s.ServeHTTP)
		return nil
	})
}

// Route registers an HTTP router.
func (s *Server) Route(prefix string, filters ...FilterFunc) *Router {
	return newRouter(prefix, s, filters...)
}

// Handle registers a new route with a matcher for the URL path.
func (s *Server) Handle(path string, h http.Handler) {
	s.router.Handle(path, h)
}

// HandlePrefix registers a new route with a matcher for the URL path prefix.
func (s *Server) HandlePrefix(prefix string, h http.Handler) {
	s.router.PathPrefix(prefix).Handler(h)
}

// HandleFunc registers a new route with a matcher for the URL path.
func (s *Server) HandleFunc(path string, h http.HandlerFunc) {
	s.router.HandleFunc(path, h)
}

// HandleHeader registers a new route with a matcher for the header.
func (s *Server) HandleHeader(key, val string, h http.HandlerFunc) {
	s.router.Headers(key, val).Handler(h)
}

// ServeHTTP should write reply headers and data to the ResponseWriter and then return.
func (s *Server) ServeHTTP(res http.ResponseWriter, req *http.Request) {
	s.Handler.ServeHTTP(res, req)
}

/*
在 Kratos 的 HTTP Server 中，filter() 方法虽然名字容易与 FilterFunc（HTTP 过滤器链）混淆，但它实际上是一个 mux.MiddlewareFunc，被注册到 gorilla/mux 路由器的中间件栈中。

它的核心意义是：作为每个请求进入业务处理前的"基础设施层"，负责构建 Kratos 统一的传输层上下文（Transport Context）。

具体来说，它做了以下三件关键事情：

统一管理请求超时
if s.timeout > 0 {
    ctx, cancel = context.WithTimeout(req.Context(), s.timeout)
} else {
    ctx, cancel = context.WithCancel(req.Context())
}
defer cancel()

如果配置了 Timeout，则为每个请求创建带超时的 context，防止慢请求无限占用资源。
即使没有配置超时，也创建一个可取消的 context，确保请求结束时能正确释放资源。
注意：这个超时是 Kratos 框架层面的，独立于 http.Server.ReadTimeout/WriteTimeout，作用于整个中间件+业务处理链路。

提取路径模板
pathTemplate := req.URL.Path
if route := mux.CurrentRoute(req); route != nil {
    pathTemplate, _ = route.GetPathTemplate()
}

将 /users/123/orders/456 这样的具体路径还原为 /users/{id}/orders/{oid} 这样的模板形式。
这个模板后续会作为 operation 字段写入 Transport，用于日志、监控指标聚合、链路追踪等场景（避免高基数问题）。

构建并注入 Transport 上下文 ⭐️ 最核心的作用
tr := &Transport{
    operation:    pathTemplate,
    pathTemplate: pathTemplate,
    reqHeader:    headerCarrier(req.Header),   // ← 你之前问的 headerCarrier
    replyHeader:  headerCarrier(w.Header()),
    request:      req,
    response:     w,
}
tr.request = req.WithContext(transport.NewServerContext(ctx, tr))
next.ServeHTTP(w, tr.request)

这里完成了 Kratos 框架抽象的关键一步：
组件   作用
headerCarrier(req.Header)   将 http.Header 包装为满足 transport.Header 接口的类型，使中间件可以统一读写请求头

headerCarrier(w.Header())   同理包装响应头，让中间件能在业务处理前/后操作响应头

transport.NewServerContext   将 Transport 嵌入 context，使得任何下游代码都可以通过 transport.FromServerContext(ctx) 获取传输层信息

为什么叫 "filter" 而不是 "middleware"？

这是历史命名遗留。在 Kratos 的设计中：
FilterFunc = 纯 HTTP 层面的过滤器（类似 net/http middleware），通过 FilterChain 包裹在最外层
filter() 返回的 mux.MiddlewareFunc = 框架内部的上下文构建器，运行在 mux 路由匹配之后、业务 handler 之前

执行顺序实际上是：
请求进入
  → FilterChain (s.filters...)        ← 用户自定义 HTTP 过滤器
    → mux.Router
      → filter() (本函数)             ← 构建 Transport 上下文
        → 路由匹配的 Handler          ← 业务逻辑

与你之前问题的关联

这正是 headerCarrier 存在的意义所在。filter() 需要将原始的 http.Header 适配成 Kratos 的 transport.Header 接口，而 headerCarrier 就是那个适配器。这样 Kratos 的中间件体系就可以完全不依赖 net/http，实现传输协议无关的设计——同一套中间件既能用于 HTTP Server，也能用于 gRPC Server。

一句话总结：filter() 是 Kratos HTTP Server 的"上下文工厂"，它将原始的 HTTP 请求转化为框架统一的 Transport 抽象，使上层的中间件、日志、监控等基础设施能够以协议无关的方式工作。
*/

func (s *Server) filter() mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			var (
				ctx    context.Context
				cancel context.CancelFunc
			)
			if s.timeout > 0 {
				ctx, cancel = context.WithTimeout(req.Context(), s.timeout)
			} else {
				ctx, cancel = context.WithCancel(req.Context())
			}
			defer cancel()

			pathTemplate := req.URL.Path
			if route := mux.CurrentRoute(req); route != nil {
				// /path/123 -> /path/{id}
				pathTemplate, _ = route.GetPathTemplate()
			}

			tr := &Transport{
				operation:    pathTemplate,
				pathTemplate: pathTemplate,
				reqHeader:    headerCarrier(req.Header),
				replyHeader:  headerCarrier(w.Header()),
				request:      req,
				response:     w,
			}
			if s.endpoint != nil {
				tr.endpoint = s.endpoint.String()
			}
			tr.request = req.WithContext(transport.NewServerContext(ctx, tr))

			/*
							没错，确实互相循环了，但这在 Go 中完全没有问题。

				让我用最直白的方式解释为什么"循环引用 ≠ 内存泄漏"：

				你的担忧本质上是 Python/C++ 的思维

				在引用计数 GC 的语言中：
				tr 引用 req → req 引用数 +1
				req(通过ctx) 引用 tr → tr 引用数 +1
				→ 两者引用数永远 ≥ 1 → 永远无法释放 → 💥 内存泄漏

				Go 的 GC 根本不数引用

				Go 用的是 Mark-and-Sweep，它的逻辑是：

				"从所有活跃变量（goroutine 栈、全局变量等）出发，能走到的对象就是活的；走不到的，不管内部怎么互指，全部回收。"

				请求处理完毕后的状态：

				GC Root (goroutine stack, globals...)
				       │
				       ✗ 没有任何路径指向 tr
				       ✗ 没有任何路径指向 req
				       ✗ 没有任何路径指向 ctx

				    ┌──────────────────────┐
				    │  tr ←→ req ←→ ctx   │  ← 三者互相引用
				    │  (孤岛，不可达)       │
				    └──────────────────────┘
				         ↓
				      整体回收 ✅

				关键点：GC 不关心环内有多少条边，只关心从 Root 能不能到达这个环。 请求结束后，整个环变成孤岛，一次 GC 就全部清掉。

				这不是 Kratos 的特殊设计，而是 Go HTTP 的标准范式

				标准库自己的 middleware 模式也是这么做的：

				// net/http 官方推荐写法
				func authMiddleware(next http.Handler) http.Handler {
				    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				        user := authenticate(r)
				        // r.WithContext 把 user 塞进 context
				        // context 挂在 r 上
				        // 如果 user 里又存了 r → 同样的环
				        ctx := context.WithValue(r.Context(), userKey, user)
				        next.ServeHTTP(w, r.WithContext(ctx))
				    })
				}

				Go 社区十几年来一直这么写，从未因此出过内存泄漏。

				一句话总结

				循环引用在引用计数语言里是 bug，在 Mark-and-Sweep 语言里是正常数据结构。 Go 属于后者，tr ↔ req ↔ ctx 的环在请求结束后变为不可达孤岛，会被 GC 完整回收，不存在任何问题。
			*/
			next.ServeHTTP(w, tr.request)
		})
	}
}

// Endpoint return a real address to registry endpoint.
// examples:
//
//	https://127.0.0.1:8000
//	Legacy: http://127.0.0.1:8000?isSecure=false
func (s *Server) Endpoint() (*url.URL, error) {
	if err := s.listenAndEndpoint(); err != nil {
		return nil, err
	}
	return s.endpoint, nil
}

// Start start the HTTP server.
func (s *Server) Start(ctx context.Context) error {
	if err := s.listenAndEndpoint(); err != nil {
		return err
	}
	s.BaseContext = func(net.Listener) context.Context {
		return ctx
	}
	log.Info("[HTTP] server listening", "addr", s.lis.Addr().String())
	var err error
	if s.tlsConf != nil {
		err = s.ServeTLS(s.lis, "", "")
	} else {
		err = s.Serve(s.lis)
	}
	if !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Stop stop the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	log.Info("[HTTP] server stopping")
	err := s.Shutdown(ctx)
	if err != nil {
		if ctx.Err() != nil {
			log.Warn("[HTTP] server couldn't stop gracefully in time, doing force stop")
			err = s.Close()
		}
	}
	return err
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
		s.endpoint = endpoint.NewEndpoint(endpoint.Scheme("http", s.tlsConf != nil), addr)
	}
	return s.err
}
