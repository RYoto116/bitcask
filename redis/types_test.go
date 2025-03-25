package redis

import (
	"bitcask"
	"bitcask/utils"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisDataStructure_Get(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	err = rds.Set(utils.GetTestKey(1), 0, utils.RandomValue(5))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(5))
	assert.Nil(t, err)

	val1, err := rds.Get(utils.GetTestKey(1))
	assert.NotNil(t, val1)
	assert.Nil(t, err)

	val2, err := rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	time.Sleep(time.Second * 5)
	val2, err = rds.Get(utils.GetTestKey(2))
	assert.Nil(t, err)
	assert.Nil(t, val2)

	val3, err := rds.Get(utils.GetTestKey(3))
	assert.Nil(t, val3)
	assert.Equal(t, err, bitcask.ErrKeyNotFound)
}

func TestRedisDataStructure_Del_Type(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-del-type")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	err = rds.Set(utils.GetTestKey(1), time.Second*5, utils.RandomValue(5))
	assert.Nil(t, err)
	err = rds.Set(utils.GetTestKey(2), time.Second*5, utils.RandomValue(5))
	assert.Nil(t, err)

	err = rds.Del(utils.GetTestKey(1))
	assert.Nil(t, err)

	val, err := rds.Get(utils.GetTestKey(1))
	assert.Nil(t, val)
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

	typ, err := rds.Type(utils.GetTestKey(1))
	assert.Equal(t, typ, byte(0))
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

	typ, err = rds.Type(utils.GetTestKey(2))
	assert.Equal(t, typ, String)
	assert.Nil(t, err)
}

func TestRedisDataStructure_HGet(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hget")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	val, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	t.Log(val, err)

	ok, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(5))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(5))
	assert.Nil(t, err)
	assert.True(t, ok)

	val1, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, err)
	assert.NotNil(t, val1)
	val2, err := rds.HGet(utils.GetTestKey(1), []byte("field2"))
	assert.Nil(t, err)
	assert.NotNil(t, val2)

	ok, err = rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(5))
	assert.Nil(t, err)
	assert.False(t, ok)

	val3, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.NotEqual(t, val3, val2)
	assert.Nil(t, err)

	val, err = rds.HGet(utils.GetTestKey(1), []byte("field3"))
	t.Log(val, err)
}

func TestRedisDataStructure_HDel(t *testing.T) {
	opts := bitcask.DefaultOptions
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-hdel")
	opts.DirPath = dir

	rds, err := NewRedisDataStructure(opts)
	assert.Nil(t, err)
	assert.NotNil(t, rds)

	ok, err := rds.HSet(utils.GetTestKey(1), []byte("field1"), utils.RandomValue(5))
	assert.Nil(t, err)
	assert.True(t, ok)
	ok, err = rds.HSet(utils.GetTestKey(1), []byte("field2"), utils.RandomValue(5))
	assert.Nil(t, err)
	assert.True(t, ok)

	val, err := rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.NotNil(t, val)
	assert.Nil(t, err)

	exist, err := rds.HDel(utils.GetTestKey(1), []byte("field1"))
	assert.True(t, exist)
	assert.Nil(t, err)

	val, err = rds.HGet(utils.GetTestKey(1), []byte("field1"))
	assert.Nil(t, val)
	assert.Equal(t, err, bitcask.ErrKeyNotFound)

	exist, err = rds.HDel(utils.GetTestKey(1), []byte("field2"))
	assert.True(t, exist)
	assert.Nil(t, err)

	exist, err = rds.HDel(utils.GetTestKey(2), []byte("field2"))
	assert.False(t, exist)
	assert.Nil(t, err)
}
