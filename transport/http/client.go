package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-kratos/kratos/v3/encoding"
	"github.com/go-kratos/kratos/v3/errors"
	"github.com/go-kratos/kratos/v3/internal/host"
	"github.com/go-kratos/kratos/v3/internal/httputil"
	"github.com/go-kratos/kratos/v3/middleware"
	"github.com/go-kratos/kratos/v3/registry"
	"github.com/go-kratos/kratos/v3/selector"
	"github.com/go-kratos/kratos/v3/selector/wrr"
	"github.com/go-kratos/kratos/v3/transport"
)

/*
这三个概念分别解决了微服务调用链中三个完全不同维度的问题。简单来说，它们是为了防止把“找服务”、“挑实例”和“防抖动”这三件事混在一起做。

以下是它们各自存在的根本原因及协作关系：

Registry / Discovery（服务发现）
解决的问题： “世界上有哪些可用的服务实例？”
为什么需要它： 在微服务架构中，实例是动态的（随时扩缩容、重启、漂移）。调用方不可能硬编码 IP。Discovery 是一个被动接收全量/增量数据的机制，它只负责忠实地同步注册中心的状态，不关心你要怎么用它。
产出物： []*registry.ServiceInstance（包含 IP、端口、Metadata、Version 等全量原始信息）。
类比： 电话簿。它记录了所有人的号码和地址，但不会告诉你今天该打给谁。

Selector / Node（负载均衡选择器）
解决的问题： “在有这么多可用实例的情况下，这一次请求具体发给谁？”
为什么需要它： Discovery 给你 100 个实例，但你一次只能调一个。你需要一个算法（P2C、WRR、Random、ConsistentHash）来做决策。同时，不同的协议（HTTP/gRPC）需要不同的连接地址，Node 就是对“可路由端点”的标准化抽象。
产出物： selector.Node（仅包含协议、Address、Weight 等路由必需字段）。
类比： 你的大脑根据“谁最近空闲”、“谁离我近”等策略，从电话簿里挑出一个具体的号码拨出去。

Subset（子集分片）
解决的问题： “当实例数量极大时，如何避免每个客户端都持有全量连接导致的资源爆炸？”
为什么需要它： 这是大厂在生产环境中踩坑后引入的关键优化。假设你有 1000 个下游实例，如果每个上游 Pod 都和这 1000 个建立长连接，连接数就是 N times M 级别，会导致：
    客户端内存/FD 耗尽
    服务端连接风暴
    服务发现推送的数据量过大
    Subset 通过一致性哈希，让每个客户端只固定连接到其中一小部分实例（如 50 个），在保证负载均衡的同时将连接复杂度从 O(N times M) 降到 O(N times K)。
位置： 夹在 Discovery 和 Selector 之间，作为一层过滤器。
类比： 你认识 1000 个人，但你只固定和其中 50 个人保持日常联系。需要找人时，优先在这 50 人里挑；只有这 50 人都不可用时，才去翻完整电话簿。

三者的协作流水线

Registry (全量)
    │  ① 同步所有实例
    ▼
[]*ServiceInstance
    │  ② Subset 过滤 (按 selectorKey + subsetSize 取子集)
    ▼
[]*ServiceInstance (子集)
    │  ③ 提取 Endpoint + 转换为 Node
    ▼
[]selector.Node
    │  ④ Rebalancer.Apply() 更新负载均衡器状态
    ▼
Selector.Select() → 返回单个 Node → 发起 RPC 调用

为什么不合并成一个概念？
如果合并...   会导致的问题
Discovery + Selector   负载均衡算法被绑定到特定注册中心的数据结构上，无法复用 P2C/WRR 等通用算法

Selector + Subset   不需要 Subset 的小规模场景被迫承担分片计算的开销；Subset 策略无法独立替换

Discovery + Subset   Subset 逻辑侵入服务同步流程，导致不同注册中心的 Watcher 实现都要重复写分片代码

总结：
Discovery 是数据源（What exists）
Subset 是规模优化（What I care about）
Selector/Node 是决策引擎（Who to call now）

三者正交解耦，使得 Kratos 可以独立演进每一层：换注册中心不影响 LB 算法，调 Subset 大小不改 Selector 代码，新增 LB 策略无需触碰服务发现逻辑。这就是良好分层架构的价值。
*/

func init() {
	if selector.GlobalSelector() == nil {
		selector.SetGlobalSelector(wrr.NewBuilder())
	}
}

// DecodeErrorFunc is decode error func.
type DecodeErrorFunc func(ctx context.Context, res *http.Response) error

// EncodeRequestFunc is request encode func.
type EncodeRequestFunc func(ctx context.Context, contentType string, in any) (body []byte, err error)

// DecodeResponseFunc is response decode func.
type DecodeResponseFunc func(ctx context.Context, res *http.Response, out any) error

// ClientOption is HTTP client option.
type ClientOption func(*clientOptions)

// Client is an HTTP transport client.
type clientOptions struct {
	ctx          context.Context
	tlsConf      *tls.Config
	timeout      time.Duration
	endpoint     string
	userAgent    string
	encoder      EncodeRequestFunc
	decoder      DecodeResponseFunc
	errorDecoder DecodeErrorFunc
	transport    http.RoundTripper
	nodeFilters  []selector.NodeFilter
	discovery    registry.Discovery
	middleware   []middleware.Middleware
	block        bool
	subsetSize   int
}

// WithSubset with client discovery subset size.
// zero value means subset filter disabled
func WithSubset(size int) ClientOption {
	return func(o *clientOptions) {
		o.subsetSize = size
	}
}

// WithTransport with client transport.
func WithTransport(trans http.RoundTripper) ClientOption {
	return func(o *clientOptions) {
		o.transport = trans
	}
}

// WithTimeout with client request timeout.
func WithTimeout(d time.Duration) ClientOption {
	return func(o *clientOptions) {
		o.timeout = d
	}
}

// WithUserAgent with client user agent.
func WithUserAgent(ua string) ClientOption {
	return func(o *clientOptions) {
		o.userAgent = ua
	}
}

// WithMiddleware with client middleware.
func WithMiddleware(m ...middleware.Middleware) ClientOption {
	return func(o *clientOptions) {
		o.middleware = m
	}
}

// WithEndpoint with client addr.
func WithEndpoint(endpoint string) ClientOption {
	return func(o *clientOptions) {
		o.endpoint = endpoint
	}
}

// WithRequestEncoder with client request encoder.
func WithRequestEncoder(encoder EncodeRequestFunc) ClientOption {
	return func(o *clientOptions) {
		o.encoder = encoder
	}
}

// WithResponseDecoder with client response decoder.
func WithResponseDecoder(decoder DecodeResponseFunc) ClientOption {
	return func(o *clientOptions) {
		o.decoder = decoder
	}
}

// WithErrorDecoder with client error decoder.
func WithErrorDecoder(errorDecoder DecodeErrorFunc) ClientOption {
	return func(o *clientOptions) {
		o.errorDecoder = errorDecoder
	}
}

// WithDiscovery with client discovery.
func WithDiscovery(d registry.Discovery) ClientOption {
	return func(o *clientOptions) {
		o.discovery = d
	}
}

// WithNodeFilter with select filters
func WithNodeFilter(filters ...selector.NodeFilter) ClientOption {
	return func(o *clientOptions) {
		o.nodeFilters = filters
	}
}

// WithBlock with client block.
func WithBlock() ClientOption {
	return func(o *clientOptions) {
		o.block = true
	}
}

// WithTLSConfig with tls config.
func WithTLSConfig(c *tls.Config) ClientOption {
	return func(o *clientOptions) {
		o.tlsConf = c
	}
}

// Client is an HTTP client.
type Client struct {
	opts     clientOptions
	target   *Target
	r        *resolver
	cc       *http.Client
	insecure bool
	selector selector.Selector
}

// NewClient returns an HTTP client.
func NewClient(ctx context.Context, opts ...ClientOption) (*Client, error) {
	options := clientOptions{
		ctx:          ctx,
		timeout:      2000 * time.Millisecond,
		encoder:      DefaultRequestEncoder,
		decoder:      DefaultResponseDecoder,
		errorDecoder: DefaultErrorDecoder,
		transport:    http.DefaultTransport,
		subsetSize:   25,
	}
	for _, o := range opts {
		o(&options)
	}
	if options.tlsConf != nil {
		if tr, ok := options.transport.(*http.Transport); ok {
			cloned := tr.Clone()
			cloned.TLSClientConfig = options.tlsConf
			options.transport = cloned
		}
	}
	insecure := options.tlsConf == nil
	target, err := parseTarget(options.endpoint, insecure)
	if err != nil {
		return nil, err
	}
	selector := selector.GlobalSelector().Build()
	var r *resolver
	if options.discovery != nil {
		if target.Scheme == schemeDiscovery {
			if r, err = newResolver(ctx, options.discovery, target, selector, options.block, insecure, options.subsetSize); err != nil {
				return nil, fmt.Errorf("[http client] new resolver failed for endpoint %q: %w", options.endpoint, err)
			}
		} else if _, _, err := host.ExtractHostPort(options.endpoint); err != nil {
			return nil, fmt.Errorf("[http client] invalid endpoint format %q: %w", options.endpoint, err)
		}
	}
	return &Client{
		opts:     options,
		target:   target,
		insecure: insecure,
		r:        r,
		cc: &http.Client{
			Timeout:   options.timeout,
			Transport: options.transport,
		},
		selector: selector,
	}, nil
}

// Invoke makes a rpc call procedure for remote service.
func (client *Client) Invoke(ctx context.Context, method, path string, args any, reply any, opts ...CallOption) error {
	var (
		contentType string
		body        io.Reader
	)
	c := defaultCallInfo(path)
	for _, o := range opts {
		if err := o.before(&c); err != nil {
			return err
		}
	}
	if args != nil {
		data, err := client.opts.encoder(ctx, c.contentType, args)
		if err != nil {
			return err
		}
		contentType = c.contentType
		body = bytes.NewReader(data)
	}
	url := fmt.Sprintf("%s://%s%s", client.target.Scheme, client.target.Authority, path)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return err
	}
	if c.headerCarrier != nil {
		req.Header = *c.headerCarrier
	}

	if contentType != "" {
		req.Header.Set("Content-Type", c.contentType)
	}
	if c.accept != "" {
		req.Header.Set("Accept", c.accept)
	}
	if client.opts.userAgent != "" {
		req.Header.Set("User-Agent", client.opts.userAgent)
	}
	ctx = transport.NewClientContext(ctx, &Transport{
		endpoint:     client.opts.endpoint,
		reqHeader:    headerCarrier(req.Header),
		operation:    c.operation,
		request:      req,
		pathTemplate: c.pathTemplate,
	})
	return client.invoke(ctx, req, args, reply, c, opts...)
}

func (client *Client) invoke(ctx context.Context, req *http.Request, args any, reply any, c callInfo, opts ...CallOption) error {
	h := func(ctx context.Context, _ any) (any, error) {
		res, err := client.do(req.WithContext(ctx))
		if res != nil {
			cs := csAttempt{res: res}
			for _, o := range opts {
				o.after(&c, &cs)
			}
		}
		if err != nil {
			return nil, err
		}
		defer res.Body.Close()
		if err := client.opts.decoder(ctx, res, reply); err != nil {
			return nil, err
		}
		return reply, nil
	}
	var p selector.Peer
	ctx = selector.NewPeerContext(ctx, &p)
	if len(client.opts.middleware) > 0 {
		h = middleware.Chain(client.opts.middleware...)(h)
	}
	_, err := h(ctx, args)
	return err
}

// Do send an HTTP request and decodes the body of response into target.
// returns an error (of type *Error) if the response status code is not 2xx.
func (client *Client) Do(req *http.Request, opts ...CallOption) (*http.Response, error) {
	c := defaultCallInfo(req.URL.Path)
	for _, o := range opts {
		if err := o.before(&c); err != nil {
			return nil, err
		}
	}

	return client.do(req)
}

func (client *Client) do(req *http.Request) (*http.Response, error) {
	var done func(context.Context, selector.DoneInfo)
	if client.r != nil {
		var (
			err  error
			node selector.Node
		)
		if node, done, err = client.selector.Select(req.Context(), selector.WithNodeFilter(client.opts.nodeFilters...)); err != nil {
			return nil, errors.ServiceUnavailable("NODE_NOT_FOUND", err.Error())
		}
		if client.insecure {
			req.URL.Scheme = schemeHTTP
		} else {
			req.URL.Scheme = schemeHTTPS
		}
		req.URL.Host = node.Address()
		req.Host = node.Address()
	}
	resp, err := client.cc.Do(req)
	if err == nil {
		t, ok := transport.FromClientContext(req.Context())
		if ok {
			ht, ok := t.(*Transport)
			if ok {
				ht.replyHeader = headerCarrier(resp.Header)
			}
		}
		err = client.opts.errorDecoder(req.Context(), resp)
	}
	if done != nil {
		done(req.Context(), selector.DoneInfo{Err: err})
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Close tears down the Transport and all underlying connections.
func (client *Client) Close() error {
	if client.r != nil {
		return client.r.Close()
	}
	return nil
}

// DefaultRequestEncoder is an HTTP request encoder.
func DefaultRequestEncoder(_ context.Context, contentType string, in any) ([]byte, error) {
	if body, ok := httpBody(in); ok {
		return body.GetData(), nil
	}
	name := httputil.ContentSubtype(contentType)
	codec := encoding.GetCodec(name)
	if codec == nil {
		return nil, errors.BadRequest("CODEC", fmt.Sprintf("unregister Content-Type: %s", contentType))
	}
	body, err := codec.Marshal(in)
	if err != nil {
		return nil, err
	}
	return body, err
}

// DefaultResponseDecoder is an HTTP response decoder.
func DefaultResponseDecoder(_ context.Context, res *http.Response, v any) error {
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if body, ok := httpBody(v); ok {
		body.ContentType = res.Header.Get("Content-Type")
		body.Data = data
		return nil
	}
	return CodecForResponse(res).Unmarshal(data, v)
}

// DefaultErrorDecoder is an HTTP error decoder.
func DefaultErrorDecoder(_ context.Context, res *http.Response) error {
	if res.StatusCode >= 200 && res.StatusCode <= 299 {
		return nil
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err == nil {
		e := new(errors.Error)
		if err = CodecForResponse(res).Unmarshal(data, e); err == nil {
			e.Code = int32(res.StatusCode)
			return e
		}
	}
	return errors.Newf(res.StatusCode, errors.UnknownReason, "").WithCause(err)
}

// CodecForResponse get encoding.Codec via http.Response
func CodecForResponse(r *http.Response) encoding.Codec {
	codec := encoding.GetCodec(httputil.ContentSubtype(r.Header.Get("Content-Type")))
	if codec != nil {
		return codec
	}
	return encoding.GetCodec("json")
}
