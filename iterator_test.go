package bitcask

import (
	"bitcask-kv/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDB_NewIterator(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.False(t, iter.Valid())
}

func TestDB_Iterator_One_Value(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	err = db.Put(utils.GetTestKey(10), utils.GetTestKey(15))
	assert.Nil(t, err)

	iter := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter)
	assert.True(t, iter.Valid())

	val, err := iter.Value()
	assert.Nil(t, err)
	assert.Equal(t, utils.GetTestKey(10), iter.Key())
	assert.Equal(t, utils.GetTestKey(15), val)
}

func TestDB_Iterator_Multi_Values(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-iterator")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	db.Put([]byte("annde"), utils.RandomValue(10))
	db.Put([]byte("cndec"), utils.RandomValue(10))
	db.Put([]byte("aeeue"), utils.RandomValue(10))
	db.Put([]byte("bdhac"), utils.RandomValue(10))
	db.Put([]byte("bnede"), utils.RandomValue(10))

	iter1 := db.NewIterator(DefaultIteratorOptions)
	assert.NotNil(t, iter1)
	assert.True(t, iter1.Valid())

	for iter1.Rewind(); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}

	for iter1.Seek([]byte("b")); iter1.Valid(); iter1.Next() {
		assert.NotNil(t, iter1.Key())
	}

	// 反向迭代
	opts1 := DefaultIteratorOptions
	opts1.Reverse = true
	iter2 := db.NewIterator(opts1)
	for iter2.Rewind(); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}

	for iter2.Seek([]byte("b")); iter2.Valid(); iter2.Next() {
		assert.NotNil(t, iter2.Key())
	}

	// 前缀
	db.Put([]byte("aeede"), utils.RandomValue(10))
	db.Put([]byte("bqede"), utils.RandomValue(10))
	db.Put([]byte("caede"), utils.RandomValue(10))
	db.Put([]byte("dbede"), utils.RandomValue(10))
	opts2 := DefaultIteratorOptions
	opts2.Prefix = []byte("a")
	iter3 := db.NewIterator(opts2)
	for iter3.Rewind(); iter3.Valid(); iter3.Next() {
		assert.NotNil(t, iter3.Key())
	}

	opts3 := DefaultIteratorOptions
	opts3.Prefix = []byte("b")
	opts3.Reverse = true
	iter4 := db.NewIterator(opts3)
	for iter4.Rewind(); iter4.Valid(); iter4.Next() {
		assert.NotNil(t, iter4.Key())
	}
}
