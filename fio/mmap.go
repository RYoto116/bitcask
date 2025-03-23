package fio

import (
	"os"

	"golang.org/x/exp/mmap"
)

// MMap IO，内存文件映射
type MMap struct {
	readerAt *mmap.ReaderAt // go官方实现只能读数据
}

func NewMMapIOManager(fileName string) (*MMap, error) {
	if _, err := os.OpenFile(fileName, os.O_CREATE, DataFilePerm); err != nil {
		return nil, err
	}

	readerAt, err := mmap.Open(fileName)
	if err != nil {
		return nil, err
	}
	return &MMap{readerAt: readerAt}, nil
}

func (m *MMap) Read(buf []byte, offset int64) (int, error) {
	return m.readerAt.ReadAt(buf, offset)
}

// 不需要
func (m *MMap) Write(buf []byte) (int, error) {
	panic("not implemented")
}

// 不需要
func (m *MMap) Sync() error {
	panic("not implemented")
}

// 关闭文件
func (m *MMap) Close() error {
	return m.readerAt.Close()
}

func (m *MMap) Size() (int64, error) {
	return int64(m.readerAt.Len()), nil
}
