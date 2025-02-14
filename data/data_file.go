package data

import (
	"bitcask-kv/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

var ErrInvalidCRC = errors.New("invalid crc value, log record maybe corrupted")

type DataFile struct {
	FileID    uint32
	WriteOff  int64
	IoManager fio.IOManager
}

// 打开新的数据文件
func OpenDataFile(dirPath string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dirPath, fmt.Sprintf("%09d", fileId)+DataFileNameSuffix)
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}

	return &DataFile{FileID: fileId, WriteOff: 0, IoManager: ioManager}, nil
}

func (df *DataFile) Write(buf []byte) error {
	n, err := df.IoManager.Write(buf)
	if err != nil {
		return err
	}

	df.WriteOff += int64(n)
	return nil
}

// 根据偏移量从数据文件中读取LogRecord
func (df *DataFile) ReadLogRecord(offset int64) (*LogRecord, int64, error) {
	fileSize, err := df.IoManager.Size()
	if err != nil {
		return nil, 0, err
	}

	var readSize int64 = maxLogRecordHeaderSize
	// 重要
	if offset+readSize > fileSize {
		readSize = fileSize - offset
	}

	// 读取Header信息
	headerBuf, err := df.readNBytes(readSize, offset)
	if err != nil {
		return nil, 0, err
	}

	// 重要：从offset开始读取到文件末尾的情况
	header, headerSize := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}
	if header.crc == 0 && header.keySize == 0 && header.valueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize, valueSize := int64(header.keySize), int64(header.valueSize)
	var logRecordSize = headerSize + keySize + valueSize

	logRecord := &LogRecord{Type: header.logRecordType}

	if keySize > 0 || valueSize > 0 {
		kvBuf, err := df.readNBytes(keySize+valueSize, offset+headerSize)
		if err != nil {
			return nil, 0, err
		}

		// 从kvbuf中提取出key和value
		logRecord.Key = kvBuf[:keySize]
		logRecord.Value = kvBuf[keySize:]
	}

	// 校验数据有效性
	crc := getLogRecordCRC(logRecord, headerBuf[crc32.Size:headerSize])
	if crc != header.crc {
		return nil, 0, ErrInvalidCRC
	}

	return logRecord, logRecordSize, nil
}

func (df *DataFile) Sync() error {
	return df.IoManager.Sync()
}

func (df *DataFile) Close() error {
	return df.IoManager.Close()
}

func (df *DataFile) readNBytes(n int64, offset int64) (b []byte, err error) {
	b = make([]byte, n)
	_, err = df.IoManager.Read(b, offset)
	return
}
