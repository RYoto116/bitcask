package data

import (
	"encoding/binary"
	"hash/crc32"
)

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDeleted
	LogRecordTxnFinished
)

const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 数据内存索引，主要是描述数据在磁盘上的位置
type LogRecordPos struct {
	Fid    uint32 // 文件id，表示将数据存储到哪个文件中
	Offset int64  // 数据存储位置在文件中的偏移量
}

// 对索引位置进行编码
func EncodeLogRecordPos(pos *LogRecordPos) []byte {
	buf := make([]byte, binary.MaxVarintLen32+binary.MaxVarintLen64) // Fid: uint32, Offset: int64
	var index = 0
	index += binary.PutVarint(buf[index:], int64(pos.Fid))
	index += binary.PutVarint(buf[index:], pos.Offset)

	return buf
}

// 解码LogRecordPos
func DecodeLogRecordPos(buf []byte) *LogRecordPos {
	var index = 0
	fileId, n := binary.Varint(buf[index:])
	index += n
	offset, n := binary.Varint(buf[index:])
	index += n

	return &LogRecordPos{
		Fid:    uint32(fileId),
		Offset: offset,
	}
}

// 写入到数据文件的记录
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

type logRecordHeader struct {
	crc           uint32
	logRecordType LogRecordType
	keySize       uint32
	valueSize     uint32
}

// 暂存事务相关的数据
type TxnRecord struct {
	Record *LogRecord
	Pos    *LogRecordPos
}

// 将LogRecord编码为字节数组，返回数组长度
//
//	|  crc  |  type  |  keySize  |  valueSize  |  key  |  value  |
func EncodeLogRecord(logRecord *LogRecord) ([]byte, int64) {
	header := make([]byte, maxLogRecordHeaderSize)

	header[4] = logRecord.Type
	var index = 5

	index += binary.PutVarint(header[index:], int64(len(logRecord.Key)))
	index += binary.PutVarint(header[index:], int64(len(logRecord.Value)))

	size := index + len(logRecord.Key) + len(logRecord.Value)
	encBytes := make([]byte, size)

	copy(encBytes[:index], header[:index])
	copy(encBytes[index:], logRecord.Key)
	copy(encBytes[index+len(logRecord.Key):], logRecord.Value)

	crc := crc32.ChecksumIEEE(encBytes[4:])
	binary.LittleEndian.PutUint32(encBytes[:4], crc)

	// fmt.Printf("header length: %d, crc: %d\n", index, crc)

	return encBytes, int64(size)
}

func decodeLogRecordHeader(buf []byte) (*logRecordHeader, int64) {
	if len(buf) <= 4 {
		return nil, 0
	}

	header := &logRecordHeader{
		crc:           binary.LittleEndian.Uint32(buf[:4]),
		logRecordType: buf[4],
	}

	var index = 5
	keySize, n := binary.Varint(buf[index:])
	index += n
	valueSize, n := binary.Varint(buf[index:])
	index += n

	header.keySize, header.valueSize = uint32(keySize), uint32(valueSize)

	return header, int64(index)
}

func getLogRecordCRC(lr *LogRecord, header []byte) uint32 {
	if lr == nil {
		return 0
	}

	// 重要
	encBytes := make([]byte, len(header)+len(lr.Key)+len(lr.Value))

	copy(encBytes[:len(header)], header)
	copy(encBytes[len(header):], lr.Key)
	copy(encBytes[len(header)+len(lr.Key):], lr.Value)

	return crc32.ChecksumIEEE(encBytes)
}
