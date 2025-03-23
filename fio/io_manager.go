package fio

const DataFilePerm = 0644

type FileIOType uint8

const (
	StandardFIO FileIOType = iota
	MemoryMap
)

// 抽象 IO 管理接口，可以接入不同的 IO 类型
type IOManager interface {
	// 从文件指定位置读取对应数据
	Read([]byte, int64) (int, error)

	// 向文件写入字节数组
	Write([]byte) (int, error)

	// 持久化数据
	Sync() error

	// 关闭文件
	Close() error

	Size() (int64, error)
}

// 初始化IOManager，目前支持FileIO
func NewIOManager(fileName string, ioType FileIOType) (IOManager, error) {
	switch ioType {
	case StandardFIO:
		return NewFileIOManager(fileName)
	case MemoryMap:
		return NewMMapIOManager(fileName)
	default:
		panic("unsupported FileIO Type")
	}
}
