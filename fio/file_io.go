package fio

import "os"

// 标准系统文件 IO
type FileIO struct {
	fd *os.File // 系统文件描述符
}

func NewFileIOManager(fileName string) (*FileIO, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{fd: fd}, nil
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

// 向文件写入字节数组
func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

// 持久化数据
func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

// 关闭文件
func (fio *FileIO) Close() error {
	return fio.fd.Close()
}

func (fio *FileIO) Size() (int64, error) {
	stat, err := fio.fd.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}
