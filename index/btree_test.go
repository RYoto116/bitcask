package index

import (
	"bitcask/data"
	"bitcask/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBTree_Put(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 2, Offset: 3})
	assert.Equal(t, res3.Fid, uint32(1))
	assert.Equal(t, res3.Offset, int64(2))
}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})
	assert.Nil(t, res1)
	pos1 := bt.Get(nil)
	assert.Equal(t, uint32(1), pos1.Fid)
	assert.Equal(t, int64(100), pos1.Offset)
	res2 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 2})
	assert.Nil(t, res2)
	res3 := bt.Put([]byte("a"), &data.LogRecordPos{Fid: 1, Offset: 3})
	assert.NotNil(t, res3)
	pos2 := bt.Get([]byte("a"))
	assert.Equal(t, uint32(1), pos2.Fid)
	assert.Equal(t, int64(3), pos2.Offset)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	res1, ok1 := bt.Delete(nil)
	assert.False(t, ok1)
	assert.Nil(t, res1)

	_ = bt.Put(nil, &data.LogRecordPos{Fid: 1, Offset: 100})

	res2, ok2 := bt.Delete(nil)
	assert.True(t, ok2)
	assert.NotNil(t, res2)

	res3 := bt.Put([]byte("aaa"), &data.LogRecordPos{Fid: 22, Offset: 33})
	assert.Nil(t, res3)

	res4, ok4 := bt.Delete([]byte("aaa"))
	assert.True(t, ok4)
	assert.NotNil(t, res4)
}

func TestBtree_Iterator(t *testing.T) {
	bt1 := NewBTree()
	iter1 := bt1.Iterator(false)
	assert.False(t, iter1.Valid())

	// Btree 有数据的情况
	bt1.Put(utils.GetTestKey(0), &data.LogRecordPos{Fid: 0, Offset: 100})
	iter2 := bt1.Iterator(false)
	assert.True(t, iter2.Valid())
	assert.Equal(t, iter2.Key(), []byte("bitcask-key-000000000"))
	assert.Equal(t, iter2.Value().Fid, uint32(0))
	assert.Equal(t, iter2.Value().Offset, int64(100))

	// 有多条数据
	bt1.Put(utils.GetTestKey(2), &data.LogRecordPos{Fid: 0, Offset: 100})
	bt1.Put(utils.GetTestKey(1), &data.LogRecordPos{Fid: 1, Offset: 500})
	bt1.Put(utils.GetTestKey(3), &data.LogRecordPos{Fid: 2, Offset: 300})

	iter3 := bt1.Iterator(false)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	iter4 := bt1.Iterator(true)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}

	bt1.Put(utils.GetTestKey(5), &data.LogRecordPos{Fid: 2, Offset: 300})
	bt1.Put(utils.GetTestKey(7), &data.LogRecordPos{Fid: 2, Offset: 300})

	iter5 := bt1.Iterator(false)
	iter5.Seek(utils.GetTestKey(4))
	for ; iter5.Valid(); iter5.Next() {
		assert.NotNil(t, iter5.Key())
	}

	iter6 := bt1.Iterator(true)
	iter6.Seek(utils.GetTestKey(4))
	for ; iter6.Valid(); iter6.Next() {
		assert.NotNil(t, iter6.Key())
	}

	bt1.Put(utils.GetTestKey(4), &data.LogRecordPos{Fid: 2, Offset: 300})
	iter7 := bt1.Iterator(false)
	iter7.Seek(utils.GetTestKey(4))
	for ; iter7.Valid(); iter7.Next() {
		assert.NotNil(t, iter7.Key())
	}
}
