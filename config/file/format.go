package file

import "strings"

/*从文件名中提取文件扩展名（不含点号）*/
func format(name string) string {
	if idx := strings.LastIndexByte(name, '.'); idx >= 0 {
		return name[idx+1:]
	}
	return ""
}
