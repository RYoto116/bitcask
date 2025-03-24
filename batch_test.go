package bitcask

import (
	"bitcask/utils"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_WriteBatch1(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Put(utils.GetTestKey(3), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Put(utils.GetTestKey(4), utils.RandomValue(10))
	assert.Nil(t, err)

	val, err := db.Get(utils.GetTestKey(1))
	// t.Log(val, err)
	assert.Nil(t, val)
	assert.Equal(t, err, ErrKeyNotFound)

	wb.Commit()
	val1, err := db.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	wb1 := db.NewWriteBatch(DefaultWriteBatchOptions)
	wb1.Delete(utils.GetTestKey(1))

	val3, err := db.Get(utils.GetTestKey(1))
	t.Log(string(val3), err)
	assert.Nil(t, err)
	assert.NotNil(t, val3)
	val4, err := db.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val4)

	wb1.Commit()

	wb1.Delete(utils.GetTestKey(3))
	val3, err = db.Get(utils.GetTestKey(1))
	assert.Nil(t, val3)
	assert.Equal(t, err, ErrKeyNotFound)
	val4, err = db.Get(utils.GetTestKey(3))
	assert.Nil(t, err)
	assert.NotNil(t, val4)
}

func Test_WriteBatch2(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-1")
	opts.DirPath = dir
	opts.DataFileSize = 64 * 1024 * 1024
	db, err := OpenDB(opts)
	defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	wb := db.NewWriteBatch(DefaultWriteBatchOptions)
	err = wb.Put(utils.GetTestKey(1), utils.RandomValue(10))
	assert.Nil(t, err)
	err = wb.Put(utils.GetTestKey(2), utils.RandomValue(10))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)
	assert.Equal(t, db.seqNo, uint64(1))
	err = wb.Delete(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = wb.Commit()
	assert.Nil(t, err)
	assert.Equal(t, db.seqNo, uint64(2))

	err = db.Close()
	assert.Nil(t, err)

	db1, err := OpenDB(opts)
	assert.Nil(t, err)
	assert.NotNil(t, db1)

	val1, err := db1.Get(utils.GetTestKey(1))
	assert.Equal(t, err, ErrKeyNotFound)
	assert.Nil(t, val1)

	val2, err := db1.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)
}

func TestDB_WriteBatch3(t *testing.T) {
	opts := DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-batch-3")
	// dir := "/tmp/bitcask-go-batch-3"
	opts.DirPath = dir
	db, err := OpenDB(opts)
	// defer destroyDB(db)
	assert.Nil(t, err)
	assert.NotNil(t, db)

	keys := db.ListKeys()
	t.Log(len(keys))

	// wbOpts := DefaultWriteBatchOptions
	// wbOpts.MaxBatchSize = 10000000
	// wb := db.NewWriteBatch(wbOpts)
	// for i := 0; i < 500000; i++ {
	// 	err := wb.Put(utils.GetTestKey(i), utils.RandomValue(1024))
	// 	assert.Nil(t, err)
	// }
	// err = wb.Commit()
	// assert.Nil(t, err)
}
