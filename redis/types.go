package redis

import (
	"bitcask"
	"encoding/binary"
	"errors"
	"time"
)

var (
	ErrWrongTypeOperation = errors.New("WRONGTYPE operation against a key holding the wrong kind of value")
)

// Redis 数据结构服务
type RedisDataStructure struct {
	db *bitcask.DB
}

type redisDataType = byte

const (
	String redisDataType = iota + 1
	Hash
	Set
	List
	ZSet
)

func NewRedisDataStructure(opt bitcask.Options) (*RedisDataStructure, error) {
	db, err := bitcask.OpenDB(opt)
	if err != nil {
		return nil, err
	}

	return &RedisDataStructure{db: db}, nil
}

// ===============================  String ================================
// key ==>  type + expire + payload
func (rds *RedisDataStructure) Set(key []byte, ttl time.Duration, value []byte) error {
	if value == nil {
		return nil
	}

	// 编码新key：type + expire
	buf := make([]byte, binary.MaxVarintLen64+1)
	buf[0] = byte(String)

	var index = 1
	var expire int64 = 0
	if ttl != 0 {
		expire = time.Now().Add(ttl).UnixNano()
	}

	index += binary.PutVarint(buf[index:], expire)
	encValue := make([]byte, index+len(value))
	copy(encValue[:index], buf[:index])
	copy(encValue[index:], value)

	// 调用存储接口写入数据
	return rds.db.Put(key, encValue)
}

func (rds *RedisDataStructure) Get(key []byte) ([]byte, error) {
	encValue, err := rds.db.Get(key)
	if err != nil {
		return nil, err
	}

	// 解码
	dataType := encValue[0]
	if dataType != String {
		return nil, ErrWrongTypeOperation
	}

	var index = 1
	expire, size := binary.Varint(encValue[index:])
	index += size

	// value 过期
	if expire > 0 && expire <= time.Now().UnixNano() {
		return nil, nil
	}

	return encValue[index:], nil
}
