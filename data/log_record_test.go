package data

import (
	"hash/crc32"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeLogRecord(t *testing.T) {
	// 正常情况
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	res1, n := EncodeLogRecord(rec1) // crc 2532332136
	t.Log(res1)
	assert.NotNil(t, res1)
	assert.Greater(t, n, int64(5))

	// value为空
	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}
	res2, n := EncodeLogRecord(rec2) // crc 240712713
	assert.NotNil(t, res2)
	assert.Greater(t, n, int64(5))

	// 删除类型
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordDeleted,
	}
	res3, n := EncodeLogRecord(rec3) // crc 290887979
	assert.NotNil(t, res3)
	assert.Greater(t, n, int64(5))
}

func TestDecodeLogRecordHeader(t *testing.T) {
	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	h1, size := decodeLogRecordHeader(headerBuf1)
	// t.Log(h1)
	assert.NotNil(t, h1)
	assert.Equal(t, h1.crc, uint32(2532332136))
	assert.Equal(t, h1.logRecordType, LogRecordNormal)
	assert.Equal(t, h1.keySize, uint32(4))
	assert.Equal(t, h1.valueSize, uint32(10))
	assert.Equal(t, size, int64(7))

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	h2, size := decodeLogRecordHeader(headerBuf2)
	t.Log(h2)
	assert.NotNil(t, h2)
	assert.Equal(t, h2.crc, uint32(240712713))
	assert.Equal(t, h2.logRecordType, LogRecordNormal)
	assert.Equal(t, h2.keySize, uint32(4))
	assert.Equal(t, h2.valueSize, uint32(0))
	assert.Equal(t, size, int64(7))

	headerBuf3 := []byte{43, 153, 86, 17, 1, 8, 20}
	h3, size := decodeLogRecordHeader(headerBuf3)
	t.Log(h3)
	assert.NotNil(t, h3)
	assert.Equal(t, h3.crc, uint32(290887979))
	assert.Equal(t, h3.logRecordType, LogRecordDeleted)
	assert.Equal(t, h3.keySize, uint32(4))
	assert.Equal(t, h3.valueSize, uint32(10))
	assert.Equal(t, size, int64(7))
}

func TestGetLogRecordCRC(t *testing.T) {
	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}

	headerBuf1 := []byte{104, 82, 240, 150, 0, 8, 20}
	crc1 := getLogRecordCRC(rec1, headerBuf1[crc32.Size:])
	assert.Equal(t, crc1, uint32(2532332136))

	rec2 := &LogRecord{
		Key:  []byte("name"),
		Type: LogRecordNormal,
	}

	headerBuf2 := []byte{9, 252, 88, 14, 0, 8, 0}
	crc2 := getLogRecordCRC(rec2, headerBuf2[crc32.Size:])
	t.Log(crc2)
	assert.Equal(t, crc2, uint32(240712713))
}
