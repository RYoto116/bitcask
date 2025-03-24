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
	dir, _ := os.MkdirTemp("", "bitcask-go-redis-get")
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
