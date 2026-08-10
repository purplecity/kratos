package form

import (
	"net/url"
	"reflect"

	"github.com/go-playground/form/v4"
	"google.golang.org/protobuf/proto"

	"github.com/go-kratos/kratos/v3/encoding"
)

//让 Kratos 能够无缝地将 HTTP 表单数据与 Protobuf Message 或普通 Go Struct 进行相互转换。

const (
	// Name is form codec name
	Name = "x-www-form-urlencoded"
	// Null value string
	nullStr = "null"
)

var (
	encoder = form.NewEncoder()
	decoder = form.NewDecoder()
)

// This variable can be replaced with -ldflags like below:
// go build "-ldflags=-X github.com/go-kratos/kratos/v3/encoding/form.tagName=form"
var tagName = "json"

func init() {
	decoder.SetTagName(tagName)
	encoder.SetTagName(tagName)
	encoding.RegisterCodec(codec{encoder: encoder, decoder: decoder})
}

type codec struct {
	encoder *form.Encoder
	decoder *form.Decoder
}

func (c codec) Marshal(v any) ([]byte, error) {
	var vs url.Values
	var err error
	if m, ok := v.(proto.Message); ok {
		vs, err = EncodeValues(m)
		if err != nil {
			return nil, err
		}
	} else {
		vs, err = c.encoder.Encode(v)
		if err != nil {
			return nil, err
		}
	}
	for k, v := range vs {
		if len(v) == 0 {
			delete(vs, k)
		}
	}
	return []byte(vs.Encode()), nil
}

func (c codec) Unmarshal(data []byte, v any) error {
	vs, err := url.ParseQuery(string(data))
	if err != nil {
		return err
	}

	/*
									    假设 v 是 (**MyProtoMessage)(nil)，即一个指向指针的指针，且值为 nil
									    rv = reflect.ValueOf(v) → Kind == reflect.Pointer, IsNil() == true

									   rv.Type()        // 类型是 **MyProtoMessage
									   rv.Type().Elem() // 去掉一层指针 → 类型是 *MyProtoMessage

									   reflect.New(rv.Type().Elem())
									    分配一块 *MyProtoMessage 大小的内存，返回一个 reflect.Value
									    等价于 new(*MyProtoMessage)，但注意：这里 new 出来的是 *MyProtoMessage 类型的零值
									    ⚠️ 实际上更准确地说，它创建的是 MyProtoMessage 的新实例，返回指向它的指针
									    即等价于 &MyProtoMessage{}

									   rv.Set(...)
									    将新分配的地址写入 rv 所代表的那个指针变量中
									    原来 rv 指向 nil，现在指向了一个有效的 *MyProtoMessage

										考虑以下调用场景：
										var msg *pb.UserRequest  // msg == nil
								codec.Unmarshal(data, &msg) // 传入的是 **pb.UserRequest
								此时 v 的类型是 **pb.UserRequest，值为 &nil（外层指针有效，内层为 nil）。


								rv.Type().Elem() 是类型层面的操作：获取指针指向的数据类型。
				rv.Elem() 是值层面的操作：获取指针指向的实际内存值。


				rv 是一个 *UserRequest 类型的 nil 指针
				rv.Type()        // 返回 reflect.Type，表示 *UserRequest 这个类型本身
				rv.Type().Elem() // 返回 reflect.Type，表示 UserRequest（去掉一层指针后的类型）
		rv.Elem() // 返回 reflect.Value，表示该指针实际指向的那个 UserRequest 值


							 原始传入
						v  → 类型 **pb.UserRequest

						循环结束后
						rv → 类型 pb.UserRequest（值类型，非指针）
	*/

	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			rv.Set(reflect.New(rv.Type().Elem()))
		}
		rv = rv.Elem()
	}
	// 3. 优先检查原始 v 是否是 proto.Message
	if m, ok := v.(proto.Message); ok {
		return DecodeValues(m, vs)
	}
	// 4. 再检查解引用后的值是否是 proto.Message
	if m, ok := rv.Interface().(proto.Message); ok {
		return DecodeValues(m, vs)
	}

	// 5. 普通 struct 回退到 go-playground/form
	return c.decoder.Decode(v, vs)
}

func (codec) Name() string {
	return Name
}
