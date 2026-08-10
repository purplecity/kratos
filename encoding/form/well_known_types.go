package form

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// 这段代码是 Kratos 框架中专门用于处理 Protobuf Well-Known Types (WKT) 的序列化（Marshal）逻辑。

// 要理解这段代码，必须先搞清楚什么是 Well-Known Types。

// 什么是 Well-Known Types？

// 在 Protobuf 中，除了 int32、string、message 等基础类型外，Google 官方预定义了一批标准化的特殊消息类型，放在 google/protobuf/ 命名空间下。这些类型被称为 Well-Known Types (WKT)。

// 它们虽然本质上是 Message，但在 JSON/表单 等文本协议中，不应该被序列化为普通的嵌套对象，而应该被映射为特定的原生格式。
// WKT 类型   Proto 内部结构   文本协议中的标准表示   示例
// Timestamp   {seconds, nanos}   RFC 3339 时间字符串   "2026-08-10T09:00:00Z"

// Duration   {seconds, nanos}   Go duration 字符串   "3.5s", "1h30m"

// BytesValue   {value: bytes}   Base64 编码字符串   "aGVsbG8="

// FieldMask   {paths: []string}   逗号分隔的路径串   "user.name,user.age"

// Struct   {fields: map}   JSON 对象   {"key": "val"}

// *Value   各种包装类型   对应的原始值   42, "hello", true

// ⚠️ 核心问题：如果你用普通的 proto reflection 遍历字段来编码，Timestamp 会被编码成 seconds=1723280400&nanos=0，这完全不符合 HTTP 表单/JSON 的标准规范。客户端根本无法识别。

// 这段代码做了什么？

// 这段代码就是为了解决上述问题：拦截 WKT 类型，将其转换为标准的文本表示。

// marshalTimestamp — 时间戳 → RFC 3339 字符串

// // Proto 内部: {seconds: 1723280400, nanos: 500000000}
// // 输出:       "2024-08-10T09:00:00.5Z"

// 关键细节：
// 通过 FieldNumber 取字段：不用字段名，而是用编号（1=seconds, 2=nanos）。这是因为 WKT 的字段编号是协议规范固定的，比字段名更可靠。
// 范围校验：严格检查 seconds 和 nanos 是否在合法范围内，防止无效时间。
// 精度裁剪：
//         x = strings.TrimSuffix(x, "000") // 去掉多余的纳秒零
//     x = strings.TrimSuffix(x, "000") // 再截一次（从9位→6位→3位）
//     x = strings.TrimSuffix(x, ".000")// 如果全是零，连小数点也去掉

//     这确保输出符合 RFC 3339 规范：只保留 0、3、6 或 9 位小数，且不留尾部零。

// marshalDuration — 时长 → Duration 字符串

// // Proto 内部: {seconds: 3600, nanos: 500000000}
// // 输出:       "1h0m0.5s"

// 关键细节：
// 溢出检测：time.Duration 底层是 int64 纳秒，最大约 292 年。当 proto 中的 seconds 超出此范围时，直接返回 math.MaxInt64 或 math.MinInt64 的字符串表示，避免静默溢出导致错误结果。
// 符号一致性检查：proto 允许 seconds 和 nanos 符号不一致（如 {-1s, 500000000ns} = -0.5s），但 Go 的 time.Duration 要求符号一致。代码通过额外的溢出判断来处理这种边界情况。

// marshalBytes — 字节 → Base64

// // Proto 内部: {value: [104, 101, 108, 108, 111]}
// // 输出:       "aGVsbG8="

// 这是最简单的一个，直接将 bytes 字段做 Base64 编码。HTTP 表单和 JSON 都无法安全传输原始二进制数据，Base64 是标准做法。

// 为什么用 protoreflect 而不是生成的 Go 结构体？

// 你可能会问：为什么不直接这样做？

// // ❌ 不推荐
// ts := m.(*timestamppb.Timestamp)
// return ts.AsTime().Format(time.RFC3339Nano), nil

// 原因有三：
// 维度   类型断言方式   protoreflect 方式
// 依赖   必须 import 每个 WKT 的生成包   只需 protoreflect 一个包

// 通用性   每种类型写一个 switch case   统一通过 FullName + FieldNumber 处理

// 动态消息   无法处理 dynamicpb.Message   ✅ 天然支持，因为走的是反射接口

// 版本兼容   生成代码版本必须匹配   只依赖 proto 规范本身，永远兼容

// Kratos 作为框架，不能假设用户用的是哪个版本的 protobuf 生成代码，也不能对每种 WKT 都加 import 依赖。protoreflect 是唯一正确的抽象层。

// 在整体架构中的位置

// 结合你之前看的 codec.go，完整的调用链是：

// EncodeValues(proto.Message)
//     │
//     ├── 检查 msg.Descriptor().FullName()
//     │     ├── == "google.protobuf.Timestamp" → marshalTimestamp()
//     │     ├── == "google.protobuf.Duration"  → marshalDuration()
//     │     ├── == "google.protobuf.BytesValue"→ marshalBytes()
//     │     ├── == "google.protobuf.FieldMask" → marshalFieldMask()
//     │     └── 其他普通 Message → 递归遍历字段
//     │
//     └── 返回 url.Values

// 💡 总结：这段代码的本质是 Protobuf 文本映射规范的 Go 实现。它确保了 Kratos 在处理表单编码时，WKT 类型的表现与 gRPC-Gateway、protobuf-json 等官方工具完全一致，避免了"同一个 API，不同编码格式产出不同语义"的灾难性问题。

const (
	// timestamp
	timestampMessageFullname    protoreflect.FullName    = "google.protobuf.Timestamp"
	maxTimestampSeconds                                  = 253402300799
	minTimestampSeconds                                  = -6213559680013
	timestampSecondsFieldNumber protoreflect.FieldNumber = 1
	timestampNanosFieldNumber   protoreflect.FieldNumber = 2

	// duration
	durationMessageFullname    protoreflect.FullName    = "google.protobuf.Duration"
	secondsInNanos                                      = 999999999
	durationSecondsFieldNumber protoreflect.FieldNumber = 1
	durationNanosFieldNumber   protoreflect.FieldNumber = 2

	// bytes
	bytesMessageFullname  protoreflect.FullName    = "google.protobuf.BytesValue"
	bytesValueFieldNumber protoreflect.FieldNumber = 1

	// google.protobuf.Struct.
	structMessageFullname   protoreflect.FullName    = "google.protobuf.Struct"
	structFieldsFieldNumber protoreflect.FieldNumber = 1

	fieldMaskFullName protoreflect.FullName = "google.protobuf.FieldMask"
)

func marshalTimestamp(m protoreflect.Message) (string, error) {
	fds := m.Descriptor().Fields()
	fdSeconds := fds.ByNumber(timestampSecondsFieldNumber)
	fdNanos := fds.ByNumber(timestampNanosFieldNumber)

	secsVal := m.Get(fdSeconds)
	nanosVal := m.Get(fdNanos)
	secs := secsVal.Int()
	nanos := nanosVal.Int()
	if secs < minTimestampSeconds || secs > maxTimestampSeconds {
		return "", fmt.Errorf("%s: seconds out of range %v", timestampMessageFullname, secs)
	}
	if nanos < 0 || nanos > secondsInNanos {
		return "", fmt.Errorf("%s: nanos out of range %v", timestampMessageFullname, nanos)
	}
	// Uses RFC 3339, where generated output will be Z-normalized and uses 0, 3,
	// 6 or 9 fractional digits.
	t := time.Unix(secs, nanos).Local()
	x := t.Format("2006-01-02T15:04:05.000000000")
	x = strings.TrimSuffix(x, "000")
	x = strings.TrimSuffix(x, "000")
	x = strings.TrimSuffix(x, ".000")
	return x + "Z", nil
}

func marshalDuration(m protoreflect.Message) (string, error) {
	fds := m.Descriptor().Fields()
	fdSeconds := fds.ByNumber(durationSecondsFieldNumber)
	fdNanos := fds.ByNumber(durationNanosFieldNumber)

	secsVal := m.Get(fdSeconds)
	nanosVal := m.Get(fdNanos)
	secs := secsVal.Int()
	nanos := nanosVal.Int()
	d := time.Duration(secs) * time.Second
	overflow := d/time.Second != time.Duration(secs)
	d += time.Duration(nanos) * time.Nanosecond
	overflow = overflow || (secs < 0 && nanos < 0 && d > 0)
	overflow = overflow || (secs > 0 && nanos > 0 && d < 0)
	if overflow {
		switch {
		case secs < 0:
			return time.Duration(math.MinInt64).String(), nil
		case secs > 0:
			return time.Duration(math.MaxInt64).String(), nil
		}
	}
	return d.String(), nil
}

func marshalBytes(m protoreflect.Message) (string, error) {
	fds := m.Descriptor().Fields()
	fdBytes := fds.ByNumber(bytesValueFieldNumber)
	bytesVal := m.Get(fdBytes)
	val := bytesVal.Bytes()
	return base64.StdEncoding.EncodeToString(val), nil
}
