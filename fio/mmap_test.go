package fio

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMMap_Read(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	mmap, err := NewMMapIOManager(path)
	defer destroyFile(path)

	fio, err := NewFileIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, mmap)
	_, err = fio.Write([]byte("key-a"))
	assert.Nil(t, err)
	_, err = fio.Write([]byte("key-b"))
	assert.Nil(t, err)

	// 重要：mmap需要重新打开
	mmap, err = NewMMapIOManager(path)
	b1 := make([]byte, 5)
	n, err := mmap.Read(b1, 0)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-a"), b1)
	b2 := make([]byte, 5)
	n, err = mmap.Read(b2, 5)
	assert.Equal(t, 5, n)
	assert.Equal(t, []byte("key-b"), b2)
}

func TestMMap_Close(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	mmap, err := NewMMapIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)
	assert.NotNil(t, mmap)
	err = mmap.Close()
	assert.Nil(t, err)
}

func TestMMap_Size(t *testing.T) {
	path := filepath.Join("/tmp", "a.data")
	fio, err := NewFileIOManager(path)
	defer destroyFile(path)
	assert.Nil(t, err)

	fio.Write([]byte("aa"))
	fio.Write([]byte("abc"))
	fio.Write([]byte("ds"))

	mmap, err := NewMMapIOManager(path)
	assert.Nil(t, err)
	assert.NotNil(t, mmap)
	size, err := mmap.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(7), size)
}
