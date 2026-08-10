package form

import (
	"encoding/base64"
	"fmt"
	"net/url"
	"reflect"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// EncodeValues encode a message into url values.
func EncodeValues(msg any) (url.Values, error) {
	if msg == nil || (reflect.ValueOf(msg).Kind() == reflect.Pointer && reflect.ValueOf(msg).IsNil()) {
		return url.Values{}, nil
	}
	if v, ok := msg.(proto.Message); ok {
		u := make(url.Values)
		err := encodeByField(u, "", v.ProtoReflect())
		return u, err
	}
	return encoder.Encode(msg)
}

func encodeByField(u url.Values, path string, m protoreflect.Message) (finalErr error) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		var (
			key     string
			newPath string
		)
		if fd.HasJSONName() {
			key = fd.JSONName()
		} else {
			key = fd.TextName()
		}
		if path == "" {
			newPath = key
		} else {
			newPath = path + "." + key
		}
		if of := fd.ContainingOneof(); of != nil {
			if f := m.WhichOneof(of); f != nil && f != fd {
				return true
			}
		}
		switch {
		case fd.IsList():
			if v.List().Len() > 0 {
				list, err := encodeRepeatedField(fd, v.List())
				if err != nil {
					finalErr = err
				}
				for _, item := range list {
					u.Add(newPath, item)
				}
			}
		case fd.IsMap():
			if v.Map().Len() > 0 {
				m, err := encodeMapField(fd, v.Map())
				if err != nil {
					finalErr = err
				}
				for k, value := range m {
					u.Set(newPath+"["+k+"]", value)
				}
			}
		case (fd.Kind() == protoreflect.MessageKind) || (fd.Kind() == protoreflect.GroupKind):
			value, err := encodeMessage(fd.Message(), v)
			if err == nil {
				u.Set(newPath, value)
				return true
			}
			if err = encodeByField(u, newPath, v.Message()); err != nil {
				finalErr = err
			}
		default:
			value, err := EncodeField(fd, v)
			if err != nil {
				finalErr = err
			}
			u.Set(newPath, value)
		}
		return true
	})
	return
}

func encodeRepeatedField(fieldDescriptor protoreflect.FieldDescriptor, list protoreflect.List) ([]string, error) {
	var values []string
	for i := 0; i < list.Len(); i++ {
		value, err := EncodeField(fieldDescriptor, list.Get(i))
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func encodeMapField(fieldDescriptor protoreflect.FieldDescriptor, mp protoreflect.Map) (map[string]string, error) {
	m := make(map[string]string)
	mp.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		key, err := EncodeField(fieldDescriptor.MapKey(), k.Value())
		if err != nil {
			return false
		}
		value, err := EncodeField(fieldDescriptor.MapValue(), v)
		if err != nil {
			return false
		}
		m[key] = value
		return true
	})

	return m, nil
}

// EncodeField encode proto message filed
func EncodeField(fieldDescriptor protoreflect.FieldDescriptor, value protoreflect.Value) (string, error) {
	switch fieldDescriptor.Kind() {
	case protoreflect.BoolKind:
		return strconv.FormatBool(value.Bool()), nil
	case protoreflect.EnumKind:
		if fieldDescriptor.Enum().FullName() == "google.protobuf.NullValue" {
			return nullStr, nil
		}
		desc := fieldDescriptor.Enum().Values().ByNumber(value.Enum())
		return string(desc.Name()), nil
	case protoreflect.BytesKind:
		return base64.URLEncoding.EncodeToString(value.Bytes()), nil
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return encodeMessage(fieldDescriptor.Message(), value)
	default:
		return value.String(), nil
	}
}

// encodeMessage marshals the fields in the given protoreflect.Message.
// If the typeURL is non-empty, then a synthetic "@type" field is injected
// containing the URL as the value.
func encodeMessage(msgDescriptor protoreflect.MessageDescriptor, value protoreflect.Value) (string, error) {
	switch msgDescriptor.FullName() {
	case timestampMessageFullname:
		return marshalTimestamp(value.Message())
	case durationMessageFullname:
		return marshalDuration(value.Message())
	case bytesMessageFullname:
		return marshalBytes(value.Message())
	case "google.protobuf.DoubleValue", "google.protobuf.FloatValue", "google.protobuf.Int64Value", "google.protobuf.Int32Value",
		"google.protobuf.UInt64Value", "google.protobuf.UInt32Value", "google.protobuf.BoolValue", "google.protobuf.StringValue":
		fd := msgDescriptor.Fields()
		v := value.Message().Get(fd.ByName("value"))
		return fmt.Sprint(v.Interface()), nil
	case fieldMaskFullName:
		m, ok := value.Message().Interface().(*fieldmaskpb.FieldMask)
		if !ok || m == nil {
			return "", nil
		}
		for i, v := range m.Paths {
			m.Paths[i] = jsonCamelCase(v)
		}
		return strings.Join(m.Paths, ","), nil
	default:
		return "", fmt.Errorf("unsupported message type: %q", string(msgDescriptor.FullName()))
	}
}

// protoreflect 是 Protobuf 官方提供的纯反射 API。它的设计哲学是：完全脱离生成的 Go 结构体，仅通过元数据（Descriptor）和动态值（Value）来操作任何 Proto 消息。

// 这对于框架开发者（如 Kratos）至关重要，因为框架不能提前 import 用户定义的所有 Message 类型。

// 下面结合你贴的代码，逐一拆解这四个核心概念：

// MessageDescriptor：消息的“蓝图/图纸”
// 是什么：描述一个 Message 长什么样的只读元数据。包含消息名、字段列表、嵌套类型等。
// 类比：数据库的表结构（Schema），或者 JSON Schema。
// 代码中的体现：
//         // 获取消息的完整限定名，用于判断是哪种 WKT
//     msgDescriptor.FullName()
//     // 输出: "google.protobuf.Timestamp", "google.protobuf.FieldMask" 等

//     // 获取该消息下所有字段的描述符集合
//     fd := msgDescriptor.Fields()

// 关键点：它不包含任何实际数据，只包含结构定义。即使没有任何实例，你也能拿到 Descriptor。

// FieldDescriptor：字段的“蓝图/图纸”
// 是什么：描述单个字段元信息的只读对象。包含字段名、编号、类型、是否是 map/repeated 等。
// 类比：表结构中某一列的定义（列名、数据类型、约束）。
// 代码中的体现：
//         // 通过字段名获取字段描述符
//     fd := msgDescriptor.Fields().ByName("value")

//     // 获取字段的 JSON 名称（proto3 中驼峰命名）
//     fd.JSONName()   // 例如 "userName"

//     // 获取字段的文本名称（通常是 snake_case）
//     fd.TextName()   // 例如 "user_name"

//     // 获取字段的类型种类
//     fd.Kind()       // 返回 MessageKind, StringKind, Int32Kind 等

// 关键点：ByName()、ByNumber()、ByJSONName() 是三种不同的查找方式。WKT 处理通常用 ByNumber()（最稳定），业务代码常用 ByName() 或 ByJSONName()。

// Value：运行时的“动态数据容器”
// 是什么：一个联合体（Union），封装了 Proto 中任意类型的运行时值。它可以是标量（int/string/bool）、Message、List 或 Map。
// 类比：interface{} / any，但带有类型安全的方法集。
// 代码中的体现：
//         // 从 Message 中按字段描述符取出值
//     v := value.Message().Get(fd.ByName("value"))

//     // 将 Value 转为 Go 原生 interface{}，再 fmt.Sprint
//     fmt.Sprint(v.Interface())

//     // 如果知道值是 Message 类型，提取为 protoreflect.Message
//     value.Message()

// ⚠️ 核心陷阱：Value 的方法（.Int(), .String(), .Message() 等）不做类型检查。如果你对一个 StringKind 的 Value 调用 .Int()，会直接 panic。必须先通过 FieldDescriptor.Kind() 确认类型，或使用 .Interface() 安全转换。

// MessageKind：字段类型的“枚举标签”
// 是什么：protoreflect.Kind 枚举的一个值，表示某个字段的类型是嵌套消息。
// 为什么需要它：Proto 的字段类型分两大类：
//     标量类型：Int32Kind, StringKind, BoolKind 等 → 直接读写
//     复合类型：MessageKind, GroupKind → 需要进一步进入子消息操作
// 代码中的体现：
//         // 遍历字段时，先判断是不是 Message 类型
//     if fd.Kind() == protoreflect.MessageKind {
//         // 只有 MessageKind 才能安全调用 fd.Message() 获取子消息的 Descriptor
//         if msg := fd.Message(); msg.FullName() == fieldMaskFullName {
//             // ... 处理 FieldMask
//         }
//     }

// 关键点：这是类型分发的前置守卫。不对 Kind 做判断就直接操作 Value，是 protoreflect 新手最常见的 panic 来源。

// 📌 四者关系一图流

// ┌─────────────────────────────────────────────────────┐
// │              MessageDescriptor (蓝图)                │
// │  FullName(): "mypackage.UserRequest"                 │
// │                                                      │
// │  Fields() ──→ FieldDescriptor[]                      │
// │               ├── Name: "user_name"                  │
// │               ├── Kind: MessageKind ◄── 类型标签      │
// │               ├── Number: 1                          │
// │               └── Message() ──→ MessageDescriptor    │
// │                   (子消息的蓝图，递归)                  │
// └─────────────────────────────────────────────────────┘
//                       ↕ Get/Set
// ┌─────────────────────────────────────────────────────┐
// │              Value (运行时数据)                       │
// │  .Message() ──→ protoreflect.Message                 │
// │  .Int()     ──→ int64                                │
// │  .String()  ──→ string                               │
// │  .Interface()──→ any (安全兜底)                       │
// └─────────────────────────────────────────────────────┘

// 💡 回到你的代码：完整执行流程

// 以 EncodeFieldMask 为例，串起四个概念：

// m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
//     // ① 用 FieldDescriptor.Kind 判断类型
//     if fd.Kind() == protoreflect.MessageKind {

//         // ② 用 FieldDescriptor.Message() 获取子消息的 MessageDescriptor
//         if msg := fd.Message(); msg.FullName() == fieldMaskFullName {

//             // ③ 把 Value + MessageDescriptor 传给 encodeMessage
//             value, err := encodeMessage(msg, v)

//             // ④ 用 FieldDescriptor.JSONName() 获取输出用的 key 名
//             query = fd.JSONName() + "=" + value
//         }
//     }
//     return true
// })

// 记忆口诀：Descriptor 是图纸（只读元数据），Value 是砖块（运行时数据），Kind 是分类标签（决定怎么搬砖），Message 是既能当图纸又能当砖块的复合体。

// 掌握这四个概念后，你就具备了不依赖任何生成代码、纯动态操作任意 Proto 消息的能力。这也是所有 Proto 框架（gRPC-Gateway、Buf、Connect 等）底层实现的基石。

// EncodeFieldMask return field mask name=paths
func EncodeFieldMask(m protoreflect.Message) (query string) {
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() == protoreflect.MessageKind {
			if msg := fd.Message(); msg.FullName() == fieldMaskFullName {
				value, err := encodeMessage(msg, v)
				if err != nil {
					return false
				}
				if fd.HasJSONName() {
					query = fd.JSONName() + "=" + value
				} else {
					query = fd.TextName() + "=" + value
				}
				return false
			}
		}
		return true
	})
	return
}

// jsonCamelCase converts a snake_case identifier to a camelCase identifier,
// according to the protobuf JSON specification.
// references: https://github.com/protocolbuffers/protobuf-go/blob/master/encoding/protojson/well_known_types.go#L842
func jsonCamelCase(s string) string {
	var builder strings.Builder
	builder.Grow(len(s))

	wasUnderscore := false
	for i := 0; i < len(s); i++ { // proto identifiers are always ASCIIS
		c := s[i]
		if c != '_' {
			if wasUnderscore && isASCIILower(c) {
				c -= 'a' - 'A' // convert to uppercase
			}
			builder.WriteByte(c)
		}
		wasUnderscore = c == '_'
	}

	return builder.String()
}

func isASCIILower(c byte) bool {
	return 'a' <= c && c <= 'z'
}
