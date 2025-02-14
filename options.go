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
