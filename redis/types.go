package redis

import (
	"bitcask"
	"bitcask/utils"
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
// key ==>  type | expire | payload
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

// ===============================  Hash ================================
// 元数据：key ==> type | expire | version | size
// 数据部分：key | version | field ==> value
func (rds *RedisDataStructure) HSet(key, field, value []byte) (bool, error) {
	// 获取key对应元数据
	md, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	// 编码新key：key + version + field
	hk := &hashInternalKey{
		key:     key,
		version: md.version,
		field:   field,
	}

	encKey := hk.encode()

	// 查找field是否存在
	var exist bool = true
	if _, err := rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	// 使用batch保证一致性
	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	// 不存在，则更新元数据
	if !exist {
		md.size++ // 先增再写
		_ = wb.Put(key, md.encode())
	}
	// 若encKey存在也需要更新记录，因为新写入的value可能不一样
	_ = wb.Put(encKey, value)

	if err := wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) HGet(key, field []byte) ([]byte, error) {
	md, err := rds.findMetadata(key, Hash)
	if err != nil {
		return nil, err
	}

	if md.size == 0 {
		return nil, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: md.version,
		field:   field,
	}
	encKey := hk.encode()

	val, err := rds.db.Get(encKey)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}

	if err == bitcask.ErrKeyNotFound {
		return nil, nil
	}
	return val, nil
}

// 删除(key, field)，返回key原先是否存在
func (rds *RedisDataStructure) HDel(key, field []byte) (bool, error) {
	md, err := rds.findMetadata(key, Hash)
	if err != nil {
		return false, err
	}

	if md.size == 0 {
		return false, nil
	}

	hk := &hashInternalKey{
		key:     key,
		version: md.version,
		field:   field,
	}
	encKey := hk.encode()

	var exist = true
	if _, err := rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		exist = false
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	if exist {
		md.size--
		_ = wb.Put(key, md.encode())
		_ = wb.Delete(encKey)
		if err := wb.Commit(); err != nil {
			return false, err
		}
	}

	return exist, nil
}

// ===============================  Set  ================================
// SADD 向集合添加一个成员
// 元数据：key ==> type | expire | version | size
// 数据部分：key | version | member | menbersize ==> NULL
func (rds *RedisDataStructure) SAdd(key, member []byte) (bool, error) {
	md, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	sk := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := sk.encode()

	// encKey不存在则更新
	if _, err := rds.db.Get(encKey); err == bitcask.ErrKeyNotFound {
		wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
		// 更新元数据
		md.size++
		_ = wb.Put(key, md.encode())
		_ = wb.Put(encKey, nil) // Set不需要写入value
		if err := wb.Commit(); err != nil {
			return true, err
		}
		return true, nil
	}

	return false, nil
}

// 判断 member 元素是否是集合 key 的成员
func (rds *RedisDataStructure) SIsMember(key, member []byte) (bool, error) {
	md, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if md.size == 0 {
		return false, nil
	}

	sk := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := sk.encode()

	_, err = rds.db.Get(encKey)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}

	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}
	return true, nil
}

// 移除集合中一个成员
func (rds *RedisDataStructure) SRem(key, member []byte) (bool, error) {
	md, err := rds.findMetadata(key, Set)
	if err != nil {
		return false, err
	}

	if md.size == 0 {
		return false, nil
	}

	sik := &setInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}
	encKey := sik.encode()

	_, err = rds.db.Get(encKey)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return false, err
	}

	if err == bitcask.ErrKeyNotFound {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)
	md.size--
	_ = wb.Put(key, md.encode())
	_ = wb.Delete(encKey)
	if err := wb.Commit(); err != nil {
		return true, err
	}

	return true, nil
}

// ===============================  List  ================================
// 元数据：key ==> type | expire | version | size | head | tail
// 数据部分：key | version | index ==> value
// 将一个值插入到列表头部
func (rds *RedisDataStructure) LPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, true)
}

func (rds *RedisDataStructure) RPush(key, element []byte) (uint32, error) {
	return rds.pushInner(key, element, false)
}

// 返回key元数据的size
func (rds *RedisDataStructure) pushInner(key, element []byte, isLeft bool) (uint32, error) {
	md, err := rds.findMetadata(key, List)
	if err != nil {
		return 0, err
	}

	var index uint64
	if isLeft {
		index = md.head - 1
	} else {
		index = md.tail // 前闭后开
	}

	lik := &listInternalKey{
		key:     key,
		version: md.version,
		index:   index,
	}
	encKey := lik.encode()

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

	if isLeft {
		md.head--
	} else {
		md.tail++
	}
	md.size++
	_ = wb.Put(key, md.encode())
	_ = wb.Put(encKey, element)
	if err := wb.Commit(); err != nil {
		return 0, err
	}

	return md.size, nil
}

func (rds *RedisDataStructure) LPop(key []byte) ([]byte, error) {
	return rds.popInner(key, true)
}

func (rds *RedisDataStructure) RPop(key []byte) ([]byte, error) {
	return rds.popInner(key, false)
}

func (rds *RedisDataStructure) popInner(key []byte, isLeft bool) ([]byte, error) {
	md, err := rds.findMetadata(key, List)
	if err != nil {
		return nil, err
	}

	if md.size == 0 {
		return nil, nil
	}

	var index uint64
	if isLeft {
		index = md.head
	} else {
		index = md.tail - 1 // 前闭后开
	}

	lik := &listInternalKey{
		key:     key,
		version: md.version,
		index:   index,
	}
	encKey := lik.encode()

	element, err := rds.db.Get(encKey)
	if err != nil {
		return nil, err
	}

	if isLeft {
		md.head++
	} else {
		md.tail--
	}
	md.size--
	if err := rds.db.Put(key, md.encode()); err != nil {
		return nil, err
	}

	return element, nil
}

// ===============================  List  ================================
func (rds *RedisDataStructure) ZAdd(key []byte, score float64, member []byte) (bool, error) {
	md, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return false, err
	}

	zik := &zsetInternalKey{
		key:     key,
		version: md.version,
		member:  member,
		score:   score,
	}

	var exist = true
	value, err := rds.db.Get(zik.encodeWithMember())
	if err == bitcask.ErrKeyNotFound {
		exist = false
	}

	if exist && utils.FloatFromBytes(value) == score {
		return false, nil
	}

	wb := rds.db.NewWriteBatch(bitcask.DefaultWriteBatchOptions)

	// 不存在，需要更新元数据
	if !exist {
		md.size++
		_ = wb.Put(key, md.encode())
	}

	if exist {
		oldKey := &zsetInternalKey{
			key:     key,
			version: md.version,
			member:  member,
			score:   utils.FloatFromBytes(value),
		}
		_ = wb.Delete(oldKey.encodeWithMember())
	}

	// 写两条数据
	_ = wb.Put(zik.encodeWithMember(), utils.Float64ToBytes(score))
	_ = wb.Put(zik.encodeWithScore(), nil)
	if err := wb.Commit(); err != nil {
		return false, err
	}

	return !exist, nil
}

func (rds *RedisDataStructure) ZScore(key, member []byte) (float64, error) {
	md, err := rds.findMetadata(key, ZSet)
	if err != nil {
		return -1, err
	}

	if md.size == 0 {
		return -1, err
	}

	zik := &zsetInternalKey{
		key:     key,
		version: md.version,
		member:  member,
	}

	value, err := rds.db.Get(zik.encodeWithMember())
	if err != nil {
		return -1, err
	}

	return utils.FloatFromBytes(value), nil
}

// Hash、List、Set、ZSet等数据的元数据也保存在rds.db中。寻找key对应的元数据是否存在
func (rds *RedisDataStructure) findMetadata(key []byte, dataType redisDataType) (*metadata, error) {
	buf, err := rds.db.Get(key)
	if err != nil && err != bitcask.ErrKeyNotFound {
		return nil, err
	}

	var meta *metadata
	var exist = true

	if err == bitcask.ErrKeyNotFound {
		exist = false
	} else {
		meta = decodeMetadata(buf)
		// 判断数据类型
		if meta.dataType != dataType {
			return nil, ErrWrongTypeOperation
		}
		// 判断过期时间
		if meta.expire != 0 && meta.expire <= time.Now().UnixNano() {
			exist = false
		}
	}

	if !exist {
		meta = &metadata{
			dataType: dataType,
			expire:   0,
			version:  time.Now().UnixNano(),
			size:     0,
		}
		if dataType == List {
			meta.head = initialListMark
			meta.tail = initialListMark
		}
	}
	return meta, nil
}
