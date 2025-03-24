package index

import (
	"bitcask/data"
	"path/filepath"

	"go.etcd.io/bbolt"
)

const bptreeIndexFileName = "bptree-index"

var indexBucketName = []byte("bitcask-index")

// B+树索引，封装了bbolt库
type BPlusTree struct {
	tree *bbolt.DB
}

// 初始化B+树索引
func NewBPlusTree(dirPath string, syncWrites bool) *BPlusTree {
	opt := bbolt.DefaultOptions
	opt.NoSync = !syncWrites

	bptree, err := bbolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, opt)
	if err != nil {
		panic("failed to open bptree")
	}

	if err := bptree.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("failed to create bucket in bptree")
	}

	return &BPlusTree{tree: bptree}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		oldValue = bucket.Get(key)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("failed to Put value in bptree")
	}
	if len(oldValue) == 0 {
		return nil
	}
	return data.DecodeLogRecordPos(oldValue)
}

func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos

	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		encPos := bucket.Get(key)
		if len(encPos) > 0 {
			pos = data.DecodeLogRecordPos(encPos)
		}
		return nil
	}); err != nil {
		return nil
	}

	return pos
}

func (bpt *BPlusTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	var oldValue []byte
	if err := bpt.tree.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)

		if oldValue = bucket.Get(key); len(oldValue) > 0 {
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("failed to Delete value in bptree")
	}
	if len(oldValue) == 0 {
		return nil, false
	}
	return data.DecodeLogRecordPos(oldValue), true

}

func (bpt *BPlusTree) Size() int { // 索引中的数据数量
	var size int
	if err := bpt.tree.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("failed to get size in bptree")
	}
	return size
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator { // 使用bbolt的迭代器
	return newBptreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Close() error {
	return bpt.tree.Close()
}

type bptreeIterator struct {
	tx        *bbolt.Tx
	cursor    *bbolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBptreeIterator(tree *bbolt.DB, reverse bool) *bptreeIterator {
	tx, err := tree.Begin(false) // 开启迭代器事务
	if err != nil {
		panic("failed to begin a transaction")
	}

	bpti := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bpti.Rewind()

	return bpti
}

func (bpti *bptreeIterator) Rewind() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Last()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.First()
	}
}

// 查找第一个大于等于（或小于等于）目标的key，从此开始遍历
func (bpti *bptreeIterator) Seek(key []byte) {
	bpti.currKey, bpti.currValue = bpti.cursor.Seek(key)
}

// 跳转到下一个key
func (bpti *bptreeIterator) Next() {
	if bpti.reverse {
		bpti.currKey, bpti.currValue = bpti.cursor.Prev()
	} else {
		bpti.currKey, bpti.currValue = bpti.cursor.Next()
	}
}

// 是否遍历完所有key
func (bpti *bptreeIterator) Valid() bool {
	return len(bpti.currKey) != 0
}

// 当前位置的key值
func (bpti *bptreeIterator) Key() []byte {
	return bpti.currKey
}

// 当前位置的value值
func (bpti *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bpti.currValue)
}

// 关闭迭代器，释放对应资源
func (bpti *bptreeIterator) Close() {
	_ = bpti.tx.Rollback()
}
