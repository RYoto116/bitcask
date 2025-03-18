package bitcask

import (
	"bitcask-kv/data"
	"encoding/binary"
	"sync"
	"sync/atomic"
)

const nonTransactionSeqNo uint64 = 0

var txnFinKey = []byte("txn-fin")

// 原子批量写操作，保证事务原子性
type WriteBatch struct {
	options       WriteBatchOptions
	mu            *sync.Mutex
	db            *DB
	pendingWrites map[string]*data.LogRecord
}

// 初始化WriteBatch
func (db *DB) NewWriteBatch(opt WriteBatchOptions) *WriteBatch {
	return &WriteBatch{
		options:       opt,
		mu:            new(sync.Mutex),
		db:            db,
		pendingWrites: make(map[string]*data.LogRecord),
	}
}

// 批量写数据
func (wb *WriteBatch) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	logRecord := &data.LogRecord{Key: key, Value: value}
	// 暂存写记录
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 删除数据
func (wb *WriteBatch) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	// 待删除数据不存在，直接返回
	logRecordPos := wb.db.index.Get(key)
	if logRecordPos == nil {
		if wb.pendingWrites[string(key)] != nil {
			delete(wb.pendingWrites, string(key))
		}
		return nil
	}

	// 设立墓碑
	logRecord := &data.LogRecord{Key: key, Type: data.LogRecordDeleted}
	wb.pendingWrites[string(key)] = logRecord
	return nil
}

// 提交事务，将暂存的数据写到数据文件，并更新内存索引
func (wb *WriteBatch) Commit() error {
	wb.mu.Lock()
	defer wb.mu.Unlock()

	if len(wb.pendingWrites) == 0 {
		return nil
	}

	if uint(len(wb.pendingWrites)) > wb.options.MaxBatchSize {
		return ErrExceedMaxBatchSize
	}

	// 加锁保证事务提交串行化
	wb.db.mu.Lock()
	defer wb.db.mu.Unlock()

	// 获取当前最新的事务序列号
	seqNo := atomic.AddUint64(&wb.db.seqNo, 1)

	positions := make(map[string]*data.LogRecordPos)
	// 写数据到数据文件
	for _, record := range wb.pendingWrites {
		pos, err := wb.db.appendLogRecord(&data.LogRecord{ // 重要：前面加了锁，此处appendLogRecord不需要再加锁
			Key:   logRecordKeyWithSeqNo(record.Key, seqNo),
			Value: record.Value,
			Type:  record.Type,
		})
		if err != nil {
			return err
		}
		// 暂存所有日志记录的position索引
		positions[string(record.Key)] = pos
	}

	// 原子性关键：事务完成标识
	finRecord := &data.LogRecord{
		Key:  logRecordKeyWithSeqNo(txnFinKey, seqNo),
		Type: data.LogRecordTxnFinished,
	}
	if _, err := wb.db.appendLogRecord(finRecord); err != nil {
		return err
	}

	// 根据配置决定是否持久化
	if wb.options.SyncWrites && wb.db.activeFile != nil {
		if err := wb.db.activeFile.Sync(); err != nil {
			return err
		}
	}

	// 批量更新内存索引
	for _, record := range wb.pendingWrites {
		pos := positions[string(record.Key)]
		if record.Type == data.LogRecordNormal {
			wb.db.index.Put(record.Key, pos)
		} else {
			wb.db.index.Delete(record.Key) // 重要：暂存操作可能包含删除操作
		}
	}

	// 清空暂存数据，为下一次Commit做准备
	wb.pendingWrites = make(map[string]*data.LogRecord)

	return nil
}

// 带序列号的key
func logRecordKeyWithSeqNo(key []byte, seqNo uint64) []byte {
	seq := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(seq[:], int64(seqNo)) // 重要！深拷贝
	encKey := make([]byte, n+len(key))
	copy(encKey[:n], seq[:n])
	copy(encKey[n:], key)

	return encKey
}

// 解析logRecord的key，获取实际的key和事务序列号
func parseLogRecordKey(key []byte) ([]byte, uint64) {
	seqNo, n := binary.Uvarint(key)
	realKey := key[n:]
	return realKey, seqNo
}
