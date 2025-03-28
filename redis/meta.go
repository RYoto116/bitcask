package redis

import (
	"bitcask/utils"
	"encoding/binary"
	"math"
)

const (
	maxMetadataSize   = binary.MaxVarintLen64*2 + binary.MaxVarintLen32 + 1
	extraListMetaSize = binary.MaxVarintLen64 * 2

	initialListMark = math.MaxUint64 / 2
)

type metadata struct {
	dataType byte   // 数据类型
	expire   int64  // 过期时间
	version  int64  // 版本号，记录数据版本，用于快速删除一个key
	size     uint32 // key 对应的(filed, value) 数据量

	head uint64 // List 数据结构专用
	tail uint64 // List 数据结构专用
}

func (md *metadata) encode() []byte {
	var size = maxMetadataSize
	if md.dataType == List {
		size += extraListMetaSize
	}

	buf := make([]byte, size)
	buf[0] = md.dataType

	var index = 1
	index += binary.PutVarint(buf[index:], md.expire)
	index += binary.PutVarint(buf[index:], md.version)
	index += binary.PutVarint(buf[index:], int64(md.size))

	if md.dataType == List {
		index += binary.PutUvarint(buf[index:], md.head)
		index += binary.PutUvarint(buf[index:], md.tail)
	}

	return buf[:index]
}

func decodeMetadata(buf []byte) *metadata {
	if len(buf) < 1 {
		return nil
	}

	md := &metadata{}
	md.dataType = buf[0]
	var index = 1
	var n int

	md.expire, n = binary.Varint(buf[index:])
	index += n
	md.version, n = binary.Varint(buf[index:])
	index += n

	size, n := binary.Varint(buf[index:])
	md.size = uint32(size)
	index += n

	if md.dataType == List {
		head, n := binary.Uvarint(buf[index:])
		md.head = uint64(head)
		index += n

		tail, n := binary.Uvarint(buf[index:])
		md.tail = uint64(tail)
		index += n
	}

	return md
}

type hashInternalKey struct {
	key     []byte
	version int64
	field   []byte
}

func (hik *hashInternalKey) encode() []byte {
	buf := make([]byte, len(hik.key)+8+len(hik.field))
	var index = 0

	copy(buf[index:], hik.key)
	index += len(hik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(hik.version))
	index += 8

	copy(buf[index:], hik.field)
	index += len(hik.field)

	return buf[:index]
}

type setInternalKey struct {
	key     []byte
	version int64
	member  []byte
	// memberSize uint32
}

func (sik *setInternalKey) encode() []byte {
	buf := make([]byte, len(sik.key)+8+len(sik.member)+4) // 保留4个字节以编码member长度
	var index = 0

	copy(buf[index:], sik.key)
	index += len(sik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(sik.version))
	index += 8

	copy(buf[index:], sik.member)
	index += len(sik.member)

	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(sik.member)))
	index += 4

	return buf[:index]
}

type listInternalKey struct {
	key     []byte
	version int64
	index   uint64
}

func (lik *listInternalKey) encode() []byte {
	buf := make([]byte, len(lik.key)+8*2)
	var index = 0

	copy(buf[index:], lik.key)
	index += len(lik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(lik.version))
	index += 8

	binary.LittleEndian.PutUint64(buf[index:index+8], lik.index)
	index += 8

	return buf[:index]
}

type zsetInternalKey struct {
	key     []byte
	version int64
	member  []byte
	score   float64 // 实现有序
}

// key | version | member
func (zik *zsetInternalKey) encodeWithMember() []byte {
	buf := make([]byte, len(zik.key)+8+len(zik.member))
	var index = 0

	copy(buf[index:], zik.key)
	index += len(zik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zik.version))
	index += 8

	copy(buf[index:], zik.member)
	index += len(zik.member)

	return buf[:index]
}

// key | version | score | member | merberSize
func (zik *zsetInternalKey) encodeWithScore() []byte {
	scoreBuf := utils.Float64ToBytes(zik.score)
	buf := make([]byte, len(zik.key)+8+len(scoreBuf)+len(zik.member)+4)
	var index = 0

	copy(buf[index:], zik.key)
	index += len(zik.key)

	binary.LittleEndian.PutUint64(buf[index:index+8], uint64(zik.version))
	index += 8

	copy(buf[index:index+len(scoreBuf)], scoreBuf)
	index += len(scoreBuf)

	copy(buf[index:], zik.member)
	index += len(zik.member)

	binary.LittleEndian.PutUint32(buf[index:index+4], uint32(len(zik.member)))
	index += 4

	return buf[:index]
}
