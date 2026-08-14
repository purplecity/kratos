package host

import (
	"fmt"
	"net"
	"strconv"
)

// ExtractHostPort from address
func ExtractHostPort(addr string) (host string, port uint64, err error) {
	var ports string
	host, ports, err = net.SplitHostPort(addr)
	if err != nil {
		return
	}
	port, err = strconv.ParseUint(ports, 10, 16) //nolint:mnd
	return
}

/*
这个函数用于判断一个字符串是否是一个有效的、可在公网/内网路由的单播 IP 地址。它通过两个条件排除了特殊用途的 IP。

🔍 逐行解析

func isValidIP(addr string) bool {
    ip := net.ParseIP(addr)
    return ip.IsGlobalUnicast() && !ip.IsInterfaceLocalMulticast()
}

net.ParseIP(addr)
将字符串解析为 net.IP 对象。如果格式非法（如 "abc"、"999.999.999.999"），返回 nil。

⚠️ 潜在风险：如果 addr 是非法字符串，ip 为 nil，下一行调用 ip.IsGlobalUnicast() 会直接 panic。生产代码应先判空：
if ip == nil {
return false
}
ip.IsGlobalUnicast()
这是核心过滤条件。它要求 IP 必须是 全局单播地址，即排除以下所有特殊地址：
被排除的地址类型   示例   原因
Loopback   127.0.0.1, ::1   仅本机回环，不可路由

Link-Local   169.254.x.x, fe80::/10   仅本地链路有效

Private   10.x.x.x, 192.168.x.x   ❌ 注意：私有地址也被排除！

Multicast   224.0.0.0/4, ff00::/8   组播地址，非单播

Unspecified   0.0.0.0, ::   未指定地址

Documentation   192.0.2.0/24   RFC 文档专用

💡 关键认知
IsGlobalUnicast() 的名字容易误导人以为它包含"私有地址"。实际上在 Go 标准库中，Private Address 不属于 Global Unicast。如果你希望 192.168.1.1 也返回 true，这个函数不满足需求。

!ip.IsInterfaceLocalMulticast()
额外排除 接口本地组播地址：
IPv4: 224.0.0.0/24（如 224.0.0.1 所有主机、224.0.0.251 mDNS）
IPv6: ff01::/16

这类地址仅在单个网络接口内有效，连局域网都出不去。

🤔 为什么单独排除？
因为 IsGlobalUnicast() 已经排除了所有 Multicast，理论上这个条件是冗余的。加上它可能是出于防御性编程或历史兼容考虑，确保即使未来 Go 标准库对 IsGlobalUnicast 的定义发生变化，接口本地组播也一定被拦截。

✅ / ❌ 判定结果速查表
输入   ParseIP   IsGlobalUnicast   !IsInterfaceLocalMulticast   最终结果
"8.8.8.8"   ✅   ✅   ✅   true

"1.1.1.1"   ✅   ✅   ✅   true

"2001:4860:4860::8888"   ✅   ✅   ✅   true

"192.168.1.1"   ✅   ❌ (Private)   ✅   false

"10.0.0.1"   ✅   ❌ (Private)   ✅   false

"127.0.0.1"   ✅   ❌ (Loopback)   ✅   false

"0.0.0.0"   ✅   ❌ (Unspecified)   ✅   false

"224.0.0.1"   ✅   ❌ (Multicast)   ❌   false

"fe80::1"   ✅   ❌ (Link-Local)   ✅   false

"not-an-ip"   nil   —   —   💥 panic

📌 一句话总结

这个函数验证的是：该 IP 是否是一个真正的、可路由的公网单播地址（不含私有地址、回环、组播等）。如果你的业务场景需要允许内网 IP（如 192.168.x.x），应改用 !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsMulticast() && !ip.IsUnspecified() 或直接检查 ip != nil 后自行定义白名单。同时务必补上 nil 检查防止 panic。
*/

func isValidIP(addr string) bool {
	ip := net.ParseIP(addr)
	return ip.IsGlobalUnicast() && !ip.IsInterfaceLocalMulticast()
}

// Port return a real port.
func Port(lis net.Listener) (int, bool) {
	if addr, ok := lis.Addr().(*net.TCPAddr); ok {
		return addr.Port, true
	}
	return 0, false
}

/*
这个 Extract 函数的核心目标是：把用户配置的监听地址（可能是通配符）转换为一个其他节点可以真实访问的具体 IP:Port。

下面是逐行、逐逻辑块的深度解释：

函数签名与入参
func Extract(hostPort string, lis net.Listener) (string, error) {

hostPort: 用户配置的监听地址，如 "0.0.0.0:8080" 或 ":9000"。
lis: 已经创建好的 net.Listener。它的作用是提供操作系统实际分配的端口（当用户配置端口为 0 时）以及验证地址有效性。
返回值: 格式化好的 "真实IP:真实端口" 字符串，供服务注册使用。

解析用户配置的地址
	addr, port, err := net.SplitHostPort(hostPort)
	if err != nil && lis == nil {
		return "", err
	}

net.SplitHostPort 将 "0.0.0.0:8080" 拆分为 addr="0.0.0.0", port="8080"。
容错设计：如果拆分失败（比如用户只传了 "8080" 没带 host），但提供了 lis，则不立即报错，因为后面可以从 lis 中补救。只有当既解析失败又没有 listener 时，才返回错误。

从 Listener 获取真实端口
	if lis != nil {
		p, ok := Port(lis)
		if !ok {
			return "", fmt.Errorf("failed to extract port: %v", lis.Addr())
		}
		port = strconv.Itoa(p)
	}

为什么需要这一步？ 用户可能配置 ":0" 让 OS 随机分配端口，此时 port="0" 是无效的。只有通过 lis.Addr() 才能拿到绑定后的真实端口。
Port() 函数内部做了类型断言 *net.TCPAddr，如果不是 TCP 监听器（如 Unix Socket），ok=false，直接报错。
用真实端口覆盖之前解析出的 port 字符串。

快速路径：用户指定了具体 IP
	if len(addr) > 0 && (addr != "0.0.0.0" && addr != "[::]" && addr != "::") {
		return net.JoinHostPort(addr, port), nil
	}

如果 addr 是一个明确的单播 IP（如 "10.0.0.5"），说明用户知道自己在做什么，直接信任并返回。
排除三种通配符地址：
    "0.0.0.0" — IPv4 全接口监听
    "[::]" — IPv6 全接口监听（带括号形式）
    "::" — IPv6 全接口监听（不带括号形式）
只有这三种情况才需要进入下面的自动探测逻辑。

获取所有网络接口
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

调用系统 API 获取本机所有网卡列表（包括物理网卡、虚拟网卡、回环设备等）。
⚠️ 注意：返回的切片不保证按 Index 排序。

初始化遍历状态
	var (
		minIndex = 0
		ips      = make([]net.IP, 0, 1)
	)

minIndex: 记录当前已找到有效 IP 的最小网卡 Index。初始值 0 有边界风险（前文已分析）。
ips: 收集所有候选有效 IP，预分配容量 1（通常只需要一个结果）。

外层循环：遍历网卡
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}

过滤未启用的网卡：FlagUp 表示网卡处于 UP 状态。down 掉的网卡没有可用地址，直接跳过。

剪枝优化
		if iface.Index >= minIndex && len(ips) != 0 {
			continue
		}

如果已经找到了有效 IP（len(ips)!=0），且当前网卡 Index ≥ 已选网卡的 Index，说明当前网卡"不比已选的更优"，直接跳过。
目的：确保最终选择的是 Index 最小的物理网卡（通常是 eth0/en0），避免 docker0、veth 等虚拟网卡干扰。

获取网卡上的所有地址
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

一张网卡可能有多个 IP（IPv4 + IPv6 + 多个子网）。
获取失败时不中断整个流程，只是跳过这张网卡（防御性编程）。

内层循环：遍历地址并过滤
		for _, rawAddr := range addrs {
			var ip net.IP
			switch addr := rawAddr.(type) {
			case *net.IPAddr:
				ip = addr.IP
			case *net.IPNet:
				ip = addr.IP
			default:
				continue
			}

iface.Addrs() 返回的是 net.Addr 接口，实际类型可能是：
    *net.IPNet — 最常见，带掩码的地址（如 192.168.1.100/24）
    *net.IPAddr — 纯 IP 地址，无掩码
    其他类型（如 Unix 地址）— 直接跳过
通过类型断言提取出纯粹的 net.IP 对象。

验证 IP 有效性并收集
			if isValidIP(ip.String()) {
				minIndex = iface.Index
				ips = append(ips, ip)
				if ip.To4() != nil {
					break
				}
			}

isValidIP: 判断是否为可路由的全局单播地址（排除回环、链路本地、组播等）。⚠️ 如前文所述，它也会排除私有地址，在内网环境可能导致问题。
更新 minIndex: 记录当前有效 IP 所在网卡的 Index，用于后续剪枝。
收集 IP: 追加到候选列表。
IPv4 优先: ip.To4() != nil 说明是 IPv4 地址。一旦在当前网卡上找到 IPv4，立即 break 内层循环，不再检查该网卡上的 IPv6 地址。配合外层剪枝，也间接阻止了后续更大 Index 网卡的遍历。

返回结果
	if len(ips) != 0 {
		return net.JoinHostPort(ips[len(ips)-1].String(), port), nil
	}
	return "", nil

取最后一个元素：由于 IPv4 优先策略，如果存在 IPv4，它一定是最后被 append 的；如果只有 IPv6，则取最后添加的那个（即最小 Index 网卡上的 IPv6）。
拼接返回: 用 net.JoinHostPort 安全地组合 IP 和端口（自动处理 IPv6 括号）。
空结果不报错: 如果没有任何有效 IP，返回空字符串和 nil error。调用方需要自行判断空字符串的情况。这是一种宽松的设计，允许在没有合适网卡时优雅降级而非崩溃。

📌 完整数据流总结

用户输入: "0.0.0.0:0"
         ↓
SplitHostPort → addr="0.0.0.0", port="0"
         ↓
lis 提供真实端口 → port="38291"
         ↓
addr 是通配符 → 进入自动探测
         ↓
遍历网卡: lo(skip) → docker0(skip/filtered) → eth0 ✅ 192.168.1.100
         ↓
isValidIP 通过 → minIndex=2, ips=[192.168.1.100], IPv4 break
         ↓
后续网卡 veth(Index=8) → 8>=2 && len!=0 → skip
         ↓
返回: "192.168.1.100:38291" ← 注册到服务发现的真实地址
*/

// Extract returns a private addr and port.
func Extract(hostPort string, lis net.Listener) (string, error) {
	addr, port, err := net.SplitHostPort(hostPort)
	if err != nil && lis == nil {
		return "", err
	}
	if lis != nil {
		p, ok := Port(lis)
		if !ok {
			return "", fmt.Errorf("failed to extract port: %v", lis.Addr())
		}
		port = strconv.Itoa(p)
	}
	if len(addr) > 0 && (addr != "0.0.0.0" && addr != "[::]" && addr != "::") {
		return net.JoinHostPort(addr, port), nil
	}
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	var (
		minIndex = 0
		ips      = make([]net.IP, 0, 1)
	)
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Index >= minIndex && len(ips) != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, rawAddr := range addrs {
			var ip net.IP
			switch addr := rawAddr.(type) {
			case *net.IPAddr:
				ip = addr.IP
			case *net.IPNet:
				ip = addr.IP
			default:
				continue
			}
			if isValidIP(ip.String()) {
				minIndex = iface.Index
				ips = append(ips, ip)
				if ip.To4() != nil {
					break
				}
			}
		}
	}
	if len(ips) != 0 {
		return net.JoinHostPort(ips[len(ips)-1].String(), port), nil
	}
	return "", nil
}
