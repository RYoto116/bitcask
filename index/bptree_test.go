package index

import (
	"bitcask/data"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBPlusTree_Put(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-put")
	_ = os.MkdirAll(path, os.ModePerm)

	defer func() {
		_ = os.RemoveAll(path)
	}()

	tree := NewBPlusTree(path, false)
	res1 := tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res1)
	res2 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.Nil(t, res2)
	res3 := tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	assert.NotNil(t, res3)
}

func TestBPlusTree_Get(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-get")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	pos := tree.Get([]byte("not exist"))
	assert.Nil(t, pos)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	pos1 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 9884, Offset: 1232})
	pos2 := tree.Get([]byte("aac"))
	assert.NotNil(t, pos2)
}

func TestBPlusTree_Delete(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-delete")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	res1, ok1 := tree.Delete([]byte("not exist"))
	assert.False(t, ok1)
	assert.Nil(t, res1)

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	res2, ok2 := tree.Delete([]byte("aac"))
	assert.True(t, ok2)
	assert.NotNil(t, res2)

	pos1 := tree.Get([]byte("aac"))
	assert.Nil(t, pos1)
}

func TestBPlusTree_Size(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-size")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	assert.Equal(t, 0, tree.Size())

	tree.Put([]byte("aac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("abc"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acc"), &data.LogRecordPos{Fid: 123, Offset: 999})

	assert.Equal(t, 3, tree.Size())
}

func TestBPlusTree_Iterator(t *testing.T) {
	path := filepath.Join(os.TempDir(), "bptree-iter")
	_ = os.MkdirAll(path, os.ModePerm)
	defer func() {
		_ = os.RemoveAll(path)
	}()
	tree := NewBPlusTree(path, false)

	tree.Put([]byte("caac"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bbca"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("acce"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("ccec"), &data.LogRecordPos{Fid: 123, Offset: 999})
	tree.Put([]byte("bbba"), &data.LogRecordPos{Fid: 123, Offset: 999})

	iter := tree.Iterator(true)
	for iter.Rewind(); iter.Valid(); iter.Next() {
		assert.NotNil(t, iter.Key())
		assert.NotNil(t, iter.Value())
	}
}
