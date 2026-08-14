package subset

import (
	"errors"
	"hash/fnv"
	"sort"
	"strconv"
)

/*
这是一个基于 一致性哈希 (Consistent Hashing) 算法实现的确定性子集选择器。

它的核心作用是：给定一个 selectKey（如用户ID、请求TraceID）和一个服务实例列表，始终返回相同的一组固定数量的实例。这在微服务架构中常用于实现“会话亲和性”、“缓存分片”或“灰度发布”，确保相同的 key 总是路由到相同的后端节点子集。

下面按模块逐行深度解析：

入口函数 Subset

func SubsetM member []M {
    // 边界保护：如果需要的数量 <=0，或者实例总数已经 <= 需要的数量
    // 直接返回全部实例，无需哈希计算
    if num <= 0 || len(inss) <= num {
        return inss
    }

    // 构建一致性哈希环并填充实例
    circle := newConsistentM
    circle.set(inss)

    // 根据 selectKey 从环上顺时针取 n 个不重复的实例
    out, err := circle.getN(selectKey, num)
    if err != nil {
        return inss // 异常兜底：返回全量
    }
    return out
}

泛型约束 [M member]：要求传入的类型必须实现 String() string 方法，因为哈希环需要用字符串作为节点标识。
确定性保证：相同的 selectKey + 相同的 inss 列表 → 永远返回相同的子集。这是“一致性”的核心价值。
无状态设计：每次调用都新建哈希环。适用于实例列表可能动态变化的场景（如服务发现更新）。如果实例列表稳定且调用频繁，应在外部缓存 consistent 对象以避免重复构建开销。

数据结构 consistent

type consistent[M member] struct {
    circle       map[uint32]M      // 哈希值 → 实例的映射（环上的虚拟节点）
    members      map[string]bool   // 已加入的真实实例集合（去重用）
    sortedHashes []uint32          // 所有虚拟节点哈希值的有序切片（用于二分查找）
}

这是一致性哈希的经典三件套：
字段   作用   时间复杂度
circle   O(1) 通过哈希值找到对应实例   读 O(1)

members   防止同一实例被重复添加   写 O(1)

sortedHashes   支持二分查找定位起始位置   查 O(log N)

⚠️ 注意：这个结构体没有加锁。它被设计为单次使用或外部保证并发安全。如果在多 goroutine 环境下复用同一个 consistent 对象，需要自行加读写锁。

构建哈希环 set / add

func (c *consistent[M]) set(inss []M) {
    for _, ins := range inss {
        if c.members[ins.String()] {
            continue // 跳过已存在的实例
        }
        c.add(ins)
    }
}

func (c *consistent[M]) add(ins M) {
    // 每个真实节点创建 160 个虚拟节点
    for i := 0; i < 160; i++ {
        c.circle[hashKey(strconv.Itoa(i)+ins.String())] = ins
    }
    c.members[ins.String()] = true
    c.updateSortedHashes() // 每次添加后重新排序
}

🔑 为什么是 160 个虚拟节点？

这是一致性哈希解决数据倾斜问题的关键：

问题：如果每个真实节点只有 1 个哈希点，当节点数较少时（如 3 个），环上的分布极不均匀，导致某些节点承担远超平均的流量。
解决：引入虚拟节点，让每个真实节点在环上占据多个位置。160 是经验值（源自 Amazon Dynamo 论文），在内存开销和均匀度之间取得平衡。
效果：假设 3 个真实节点 × 160 = 480 个虚拟节点均匀分布在 2³² 的环上，每个真实节点实际承担的流量比例接近 1/3，标准差显著降低。

🔑 哈希键的构造

hashKey(strconv.Itoa(i) + ins.String())

将虚拟节点编号 i 拼接到实例标识前面，确保同一实例的不同虚拟节点产生不同的哈希值。例如：
"0server-a" → hash1
"1server-a" → hash2
...
"159server-a" → hash160

⚠️ updateSortedHashes 的性能隐患

func (c *consistent[M]) updateSortedHashes() {
    hashes := c.sortedHashes[:0]       // 复用底层数组，避免重新分配
    for key := range c.circle {
        hashes = append(hashes, key)
    }
    sort.Slice(hashes, func(i, j int) bool {
        return hashes[i] < hashes[j]
    })
    c.sortedHashes = hashes
}

[:0] 切片技巧：保留原 slice 的底层数组容量，只重置长度为 0，避免 GC 压力。
性能代价：每次 add 都会触发 O(V·log V) 的排序（V = 虚拟节点总数）。如果有 N 个实例，总构建成本为 O(N × V × log V)。对于 100 个实例 × 160 虚拟节点 = 16000 次排序操作。这就是为什么建议外部缓存 consistent 对象的原因。

核心查询 getN

func (c *consistent[M]) getN(key string, n int) ([]M, error) {
    if len(c.circle) == 0 {
        return nil, errEmptyCircle
    }
    // 如果请求的数量超过实际实例数，降级为返回全部
    if len(c.members) < n {
        n = len(c.members)
    }

    // ① 计算 key 的哈希值，二分查找环上的起始位置
    offset := c.search(hashKey(key))

    out := make([]M, 0, n)

    // ② 从起始位置顺时针遍历环
    for i := offset; ; i++ {
        // 环形回绕：到达末尾后回到开头
        if i >= len(c.sortedHashes) {
            i = 0
        }

        ins := c.circle[c.sortedHashes[i]]

        // ③ 去重检查：不同虚拟节点可能映射到同一真实实例
        if !contains(out, ins) {
            out = append(out, ins)
            if len(out) == n {
                return out, nil // 收集够了，立即返回
            }
        }

        // ④ 终止条件：绕环一圈回到起点，防止死循环
        if i == offset-1 || (offset == 0 && i == len(c.sortedHashes)-1) {
            break
        }
    }
    return out, nil
}

🔍 逐步推演

假设环上有虚拟节点哈希值排序后为：[100, 300, 500, 700, 900]，key 的哈希值为 400。

search(400) → 找到第一个 > 400 的位置 → index=2 (值500)，offset=2
i=2: sortedHashes[2]=500 → circle[500]=server-B → out=[server-B]
i=3: sortedHashes[3]=700 → circle[700]=server-A → out=[server-B, server-A]
i=4: sortedHashes[4]=900 → circle[900]=server-C → out=[server-B, server-A, server-C]
若 n=3，此时返回 ✅

🔑 去重的必要性

由于 160 个虚拟节点映射到同一个真实实例，顺时针遍历时很可能连续命中同一实例的不同虚拟节点。contains 确保结果集中每个真实实例只出现一次。

⚠️ contains 的 O(n²) 问题

func containsM member bool {
    for _, item := range inss {
        if item.String() == ins.String() {
            return true
        }
    }
    return false
}

这是线性扫描。当 num 较大时，整体 getN 的复杂度退化为 O(num² × V)。优化方案是用 map[string]bool 替代切片做去重判断，将单次查重从 O(num) 降为 O(1)。但在 num 通常较小（< 20）的场景下，切片的缓存友好性可能反而优于 map。

🔑 环形遍历的终止条件

if i == offset-1 || (offset == 0 && i == len(c.sortedHashes)-1) {
    break
}

这处理了两个边界情况：
一般情况：i 回绕后追上了 offset-1，说明已遍历完整环
offset=0 的特殊情况：offset-1 = -1，而 i 永远不会等于 -1，所以需要单独判断 i == len-1（即走到环的最后一个元素时停止）

如果不加这个保护，当所有虚拟节点都映射到少于 n 个真实实例时，循环将永远不会退出。

二分查找 search

func (c *consistent[M]) search(key uint32) int {
    i := sort.Search(len(c.sortedHashes), func(i int) bool {
        return c.sortedHashes[i] > key
    })
    if i >= len(c.sortedHashes) {
        i = 0 // 哈希值比环上所有节点都大，回绕到起点
    }
    return i
}

sort.Search 返回第一个满足 sortedHashes[i] > key 的索引，即顺时针方向第一个大于 key 的节点。
如果 key 大于环上最大值，i == len，回绕到 index 0，实现环形语义。
时间复杂度 O(log V)，V=虚拟节点总数。

哈希函数 hashKey

func hashKey(key string) uint32 {
    h := fnv.New32a()
    _, _ = h.Write([]byte(key))
    return h.Sum32()
}

FNV-1a 32bit：非加密哈希，速度极快，分布均匀性好，碰撞率低。
为什么不用 MD5/SHA？ 一致性哈希不需要抗碰撞攻击，只需要均匀分布。FNV-1a 比 MD5 快一个数量级，且在 2³² 空间内分布质量足够。
32bit vs 64bit：32bit 足以提供 ~42 亿个桶位，对于数百个虚拟节点的场景绰绰有余，且节省内存（sortedHashes 切片更小，缓存更友好）。

📌 整体流程图

Subset("user-123", [A, B, C, D, E], 3)
         │
         ▼
   构建一致性哈希环
   ┌─────────────────────────────┐
   │ A×160 + B×160 + C×160 +    │
   │ D×160 + E×160 = 800个虚拟节点 │
   │ sortedHashes 排序完成        │
   └─────────────────────────────┘
         │
         ▼
   hashKey("user-123") = 0x7A3F...
         │
         ▼
   search(0x7A3F...) → offset=342
         │
         ▼
   从 offset=342 顺时针遍历
   ┌──────────────────────────┐
   │ [342]→C  ✓ out=[C]      │
   │ [343]→C  ✗ 重复跳过     │
   │ [344]→A  ✓ out=[C,A]    │
   │ [345]→B  ✓ out=[C,A,B]  │
   │ len==3 → return!         │
   └──────────────────────────┘
         │
         ▼
   返回 [C, A, B]

⚠️ 生产环境注意事项

线程安全：此实现是无状态的，每次 Subset 调用都重建环。如需复用，必须加锁或使用 sync.RWMutex。
构建性能：100 实例 × 160 虚拟节点 = 16000 个哈希点，每次构建涉及排序。在服务发现变更不频繁的场景下，务必缓存 consistent 对象。
去重优化：当 num 较大（>50）时，建议将 contains 替换为 map 查找。
虚拟节点数可调：160 是默认值。实例数很多时（>1000）可适当减少；实例数很少时（<5）可适当增加以提升均匀度。
成员变更的影响：增删一个实例只会影响环上相邻区间的 key 映射，其他 key 的子集选择保持不变。这就是一致性哈希相比普通取模哈希的核心优势——最小化数据迁移。
*/

type member interface {
	String() string
}

var errEmptyCircle = errors.New("empty circle")

// Subset returns a deterministic subset for the given select key.
func Subset[M member](selectKey string, inss []M, num int) []M {
	if num <= 0 || len(inss) <= num {
		return inss
	}

	circle := newConsistent[M]()
	circle.set(inss)
	out, err := circle.getN(selectKey, num)
	if err != nil {
		return inss
	}
	return out
}

type consistent[M member] struct {
	circle       map[uint32]M
	members      map[string]bool
	sortedHashes []uint32
}

func newConsistent[M member]() *consistent[M] {
	return &consistent[M]{
		circle:  make(map[uint32]M),
		members: make(map[string]bool),
	}
}

func (c *consistent[M]) set(inss []M) {
	for _, ins := range inss {
		if c.members[ins.String()] {
			continue
		}
		c.add(ins)
	}
}

func (c *consistent[M]) add(ins M) {
	for i := 0; i < 160; i++ {
		c.circle[hashKey(strconv.Itoa(i)+ins.String())] = ins
	}
	c.members[ins.String()] = true
	c.updateSortedHashes()
}

func (c *consistent[M]) getN(key string, n int) ([]M, error) {
	if len(c.circle) == 0 {
		return nil, errEmptyCircle
	}
	if len(c.members) < n {
		n = len(c.members)
	}
	offset := c.search(hashKey(key))
	out := make([]M, 0, n)
	for i := offset; ; i++ {
		if i >= len(c.sortedHashes) {
			i = 0
		}
		ins := c.circle[c.sortedHashes[i]]
		if !contains(out, ins) {
			out = append(out, ins)
			if len(out) == n {
				return out, nil
			}
		}
		if i == offset-1 || (offset == 0 && i == len(c.sortedHashes)-1) {
			break
		}
	}
	return out, nil
}

func (c *consistent[M]) search(key uint32) int {
	i := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] > key
	})
	if i >= len(c.sortedHashes) {
		i = 0
	}
	return i
}

func (c *consistent[M]) updateSortedHashes() {
	hashes := c.sortedHashes[:0]
	for key := range c.circle {
		hashes = append(hashes, key)
	}
	sort.Slice(hashes, func(i, j int) bool {
		return hashes[i] < hashes[j]
	})
	c.sortedHashes = hashes
}

func contains[M member](inss []M, ins M) bool {
	for _, item := range inss {
		if item.String() == ins.String() {
			return true
		}
	}
	return false
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32()
}
