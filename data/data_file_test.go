package data

import (
	"bitcask/fio"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpenDataFile(t *testing.T) {
	df1, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df1)

	df2, err := OpenDataFile(os.TempDir(), 1, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df2)

	df, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df)
}

func TestDataFile_Write(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 0, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("aaa\n"))
	assert.Nil(t, err)

	err = df.Write([]byte("bbb\n"))
	assert.Nil(t, err)

	err = df.Write([]byte("ccc\n"))
	assert.Nil(t, err)
}

func TestDataFile_Close(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("aaa\n"))
	assert.Nil(t, err)

	err = df.Close()
	assert.Nil(t, err)
}

func TestDataFile_Sync(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 111, fio.StandardFIO)
	assert.Nil(t, err)
	assert.NotNil(t, df)

	err = df.Write([]byte("aaa\n"))
	assert.Nil(t, err)

	err = df.Sync()
	assert.Nil(t, err)
}

func TestDataFile_ReadLogRecord(t *testing.T) {
	df, err := OpenDataFile(os.TempDir(), 222, fio.StandardFIO)
	assert.NotNil(t, df)
	assert.Nil(t, err)

	rec1 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("bitcask-go"),
		Type:  LogRecordNormal,
	}
	res1, size1 := EncodeLogRecord(rec1) // crc 2532332136
	err = df.Write(res1)
	assert.Nil(t, err)

	var offset int64 = 0
	readRec1, readSize1, err := df.ReadLogRecord(offset)
	assert.Nil(t, err)
	assert.Equal(t, readRec1, rec1)
	assert.Equal(t, readSize1, size1)
	offset += readSize1

	t.Log(offset)

	// 多条记录
	rec2 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte("a new value"),
		Type:  LogRecordNormal,
	}
	res2, size2 := EncodeLogRecord(rec2)
	err = df.Write(res2)
	assert.Nil(t, err)

	readRec2, readSize2, err := df.ReadLogRecord(offset)
	assert.Nil(t, err)
	assert.Equal(t, readRec2, rec2)
	assert.Equal(t, readSize2, size2)
	offset += readSize2

	// 被删除数据在文件末尾
	rec3 := &LogRecord{
		Key:   []byte("name"),
		Value: []byte(""),
		Type:  LogRecordDeleted,
	}
	res3, size3 := EncodeLogRecord(rec3)
	err = df.Write(res3)
	assert.Nil(t, err)

	readRec3, readSize3, err := df.ReadLogRecord(offset)
	assert.Nil(t, err)
	assert.Equal(t, readRec3, rec3)
	assert.Equal(t, readSize3, size3)
	offset += readSize3

}
