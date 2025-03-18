package bitcask

import "os"

type Options struct {
	DirPath      string // 数据库数据目录
	DataFileSize int64
	SyncWrites   bool // 每次写数据后是否持久化
	IndexType    IndexerType
}

type IndexerType = int8

const (
	Btree IndexerType = iota + 1
	ART
)

var DefaultOptions = Options{
	DirPath:      os.TempDir(),
	DataFileSize: 256 * 1024 * 256, // 256 MB
	SyncWrites:   false,
	IndexType:    Btree,
}

// 批量写配置项
type WriteBatchOptions struct {
	// 一个批次中最大数据量
	MaxBatchSize uint

	// 提交时是否持久化数据
	SyncWrites bool
}

// 迭代器配置项，指定需要遍历的Key前缀以及遍历方向
type IteratorOptions struct {
	Prefix  []byte // 默认为空
	Reverse bool   // 默认为false正向
}

var DefaultIteratorOptions = IteratorOptions{
	Prefix:  nil,
	Reverse: false,
}

var DefaultWriteBatchOptions = WriteBatchOptions{
	MaxBatchSize: 10000,
	SyncWrites:   true,
}
