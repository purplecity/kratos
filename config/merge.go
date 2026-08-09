package config

import "fmt"

/*
这三个函数组成了一个 配置合并引擎，实现了"深度递归合并 + 写入时克隆"的语义。
这是配置库（如 Viper、Koanf 等）的核心原语：把多个配置源层层叠加，且保证各层之间互不污染。

每一层都：
遇到嵌套 map → 递归合并（保留下层未覆盖的 key）
遇到非 map 值 → 深拷贝后覆盖（不影响其他层）
最终 config 是所有层的安全叠加，任何一层的数据都不会被其他层意外修改
*/
func defaultMerge(dst, src any) error {
	dstMap, ok := dst.(*map[string]any)
	if !ok {
		return fmt.Errorf("config: merge dst must be *map[string]interface{}, got %T", dst)
	}
	srcMap, ok := convertMap(src).(map[string]any)
	if !ok {
		return fmt.Errorf("config: merge src must be map[string]interface{}, got %T", src)
	}
	if *dstMap == nil {
		*dstMap = make(map[string]any, len(srcMap))
	}
	mergeMap(*dstMap, srcMap)
	return nil
}

func mergeMap(dst, src map[string]any) {
	for key, srcValue := range src {
		if srcMap, ok := srcValue.(map[string]any); ok {
			if dstMap, ok := dst[key].(map[string]any); ok {
				mergeMap(dstMap, srcMap)
				continue
			}
		}
		dst[key] = cloneMergeValue(srcValue)
	}
}

func cloneMergeValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(val))
		mergeMap(cloned, val) //深拷贝防止引用共享
		return cloned
	case []any:
		cloned := make([]any, len(val))
		for i, item := range val {
			cloned[i] = cloneMergeValue(item)
		}
		return cloned
	default:
		return val
	}
}
