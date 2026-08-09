package config

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/go-kratos/kratos/v3/log"
)

// Reader is config reader.
type Reader interface {
	Merge(...*KeyValue) error
	Value(string) (Value, bool)
	Source() ([]byte, error)
	Resolve() error
}

type reader struct {
	opts   options
	values map[string]any
	lock   sync.Mutex
}

func newReader(opts options) Reader {
	return &reader{
		opts:   opts,
		values: make(map[string]any),
		lock:   sync.Mutex{},
	}
}

func (r *reader) Merge(kvs ...*KeyValue) error {
	merged, err := r.cloneMap()
	if err != nil {
		return err
	}
	for _, kv := range kvs {
		next := make(map[string]any)
		if err := r.opts.decoder(kv, next); err != nil {
			log.Error("failed to decode config", "error", err, "key", kv.Key, "value", string(kv.Value))
			return err
		}
		if err := r.opts.merge(&merged, convertMap(next)); err != nil {
			log.Error("failed to merge config", "error", err, "key", kv.Key, "value", string(kv.Value))
			return err
		}
	}
	r.lock.Lock()
	r.values = merged
	r.lock.Unlock()
	return nil
}

func (r *reader) Value(path string) (Value, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return readValue(r.values, path)
}

func (r *reader) Source() ([]byte, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return marshalJSON(convertMap(r.values))
}

func (r *reader) Resolve() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.opts.resolver(r.values)
}

func (r *reader) cloneMap() (map[string]any, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	return cloneMap(r.values)
}

/*
需要真正的深拷贝（嵌套 map/slice 也要独立副本）
数据结构完全由已知基础类型组成（string, int, float, bool, []any, map[string]any）
已经 Register 了所有需要的类型
不想引入第三方库（如 github.com/jinzhu/copier、github.com/mohae/deepcopy）
对性能不敏感（配置加载等低频操作）
*/

func cloneMap(src map[string]any) (map[string]any, error) {
	// https://gist.github.com/soroushjp/0ec92102641ddfc3ad5515ca76405f4d
	var buf bytes.Buffer
	gob.Register(map[string]any{})
	gob.Register([]any{})
	enc := gob.NewEncoder(&buf)
	dec := gob.NewDecoder(&buf)
	err := enc.Encode(src)
	if err != nil {
		return nil, err
	}
	var clone map[string]any
	err = dec.Decode(&clone)
	if err != nil {
		return nil, err
	}
	return clone, nil
}

func convertMap(src any) any {
	switch m := src.(type) {
	case map[string]any:
		dst := make(map[string]any, len(m))
		for k, v := range m {
			dst[k] = convertMap(v)
		}
		return dst
	case map[any]any:
		dst := make(map[string]any, len(m))
		for k, v := range m {
			dst[fmt.Sprint(k)] = convertMap(v)
		}
		return dst
	case []any:
		dst := make([]any, len(m))
		for k, v := range m {
			dst[k] = convertMap(v)
		}
		return dst
	case []byte:
		// there will be no binary data in the config data
		return string(m)
	default:
		return src
	}
}

// readValue read Value in given map[string]interface{}
// by the given path, will return false if not found.
func readValue(values map[string]any, path string) (Value, bool) {
	var (
		next = values
		keys = strings.Split(path, ".")
		last = len(keys) - 1
	)
	for idx, key := range keys {
		value, ok := next[key]
		if !ok {
			return nil, false
		}
		if idx == last {
			av := &atomicValue{}
			av.Store(value)
			return av, true
		}
		switch vm := value.(type) {
		case map[string]any:
			next = vm
		default:
			return nil, false
		}
	}
	return nil, false
}

/*
这两个函数是 JSON 序列化/反序列化的统一适配层，核心目的是：
让同一套代码同时兼容普通 Go 结构体和 Protobuf Message，且各自使用最合适的 JSON 库。
*/

func marshalJSON(v any) ([]byte, error) {
	if m, ok := v.(proto.Message); ok {
		return protojson.MarshalOptions{EmitUnpopulated: true}.Marshal(m)
	}
	return json.Marshal(v)
}

func unmarshalJSON(data []byte, v any) error {
	// m 和 v 指向同一个底层对象
	if m, ok := v.(proto.Message); ok {
		return protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(data, m)
	}
	return json.Unmarshal(data, v)
}
