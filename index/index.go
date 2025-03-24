package index

import (
	"bitcask/data"
	"bytes"

	"github.com/google/btree"
)

// 抽象索引接口，后续可以接入其他数据结构
type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) (*data.LogRecordPos, bool)
	Size() int                      // 索引中的数据数量
	Iterator(reverse bool) Iterator // 迭代器
	Close() error
}

type IndexType = int8

const (
	Btree IndexType = iota + 1
	ART             // 自适应基数树
	BPTree
)

// 根据索引类型初始化内存索引
func NewIndexer(typ IndexType, dirPath string, sync bool) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		return NewART()
	case BPTree:
		return NewBPlusTree(dirPath, sync)
	default:
		panic("unsupported index type")
	}
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (ai *Item) Less(bi btree.Item) bool {
	return bytes.Compare(ai.key, bi.(*Item).key) == -1
}

// 通用的索引迭代器
type Iterator interface {
	// 重新返回迭代器起点（第一个数据）
	Rewind()

	// 查找第一个大于等于（或小于等于）目标的key，从此开始遍历
	Seek(key []byte)

	// 跳转到下一个key
	Next()

	// 是否遍历完所有key
	Valid() bool

	// 当前位置的key值
	Key() []byte

	// 当前位置的value值
	Value() *data.LogRecordPos

	// 关闭迭代器，释放对应资源
	Close()
}
