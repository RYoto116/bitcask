package bitcask

import (
	"bitcask-kv/index"
	"bytes"
)

// 面向用户调用的迭代器接口
type Iterator struct {
	indexIterator index.Iterator // 索引迭代器，方便取出key和索引信息
	db            *DB            // 根据索引信息取出value
	options       IteratorOptions
}

// 初始化数据库迭代器
func (db *DB) NewIterator(opt IteratorOptions) *Iterator {
	indexIter := db.index.Iterator(opt.Reverse)
	return &Iterator{
		indexIterator: indexIter,
		db:            db,
		options:       opt,
	}
}

// 重新返回迭代器起点（第一个数据）
func (it *Iterator) Rewind() {
	it.indexIterator.Rewind()
	it.skipToNext()
}

// 查找第一个大于等于（或小于等于）目标的key，从此开始遍历
func (it *Iterator) Seek(key []byte) {
	it.indexIterator.Seek(key)
	it.skipToNext()
}

// 跳转到下一个key
func (it *Iterator) Next() {
	it.indexIterator.Next()
	it.skipToNext()
}

// 是否遍历完所有key
func (it *Iterator) Valid() bool {
	return it.indexIterator.Valid()
}

// 当前位置的key值
func (it *Iterator) Key() []byte {
	return it.indexIterator.Key()
}

// 当前位置的value值
func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIterator.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)
}

// 关闭迭代器，释放对应资源
func (it *Iterator) Close() {
	it.indexIterator.Close()
}

// 根据前缀要求过滤
func (it *Iterator) skipToNext() {
	prefixLen := len(it.options.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.Valid(); it.Next() {
		key := it.Key()
		if prefixLen <= len(key) && bytes.Compare(it.options.Prefix, key[:prefixLen]) == 0 {
			break
		}
	}
}
