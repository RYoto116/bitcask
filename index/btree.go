package index

import (
	"bitcask-kv/data"
	"bytes"
	"sort"
	"sync"

	btree "github.com/google/btree"
)

// 封装Google的btree库
type BTree struct {
	tree *btree.BTree // 写操作并发不安全
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	it := Item{key: key, pos: pos}
	bt.lock.Lock()
	oldItem := bt.tree.ReplaceOrInsert(&it)
	bt.lock.Unlock()
	if oldItem == nil {
		return nil
	}

	return oldItem.(*Item).pos
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := Item{key: key}
	btreeItem := bt.tree.Get(&it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	it := Item{key: key}
	bt.lock.Lock()
	defer bt.lock.Unlock()

	oldItem := bt.tree.Delete(&it)
	if oldItem == nil {
		return nil, false
	}
	return oldItem.(*Item).pos, true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}

	bt.lock.RLock()
	defer bt.lock.RUnlock()

	return newBtreeIterator(bt.tree, reverse)
}

func (bt *BTree) Close() error {
	return nil
}

// BTree 索引迭代器
type btreeIterator struct {
	curIndex int     // 当前迭代位置
	reverse  bool    // 是否反向遍历
	values   []*Item // key+位置信息
}

// 初始化btree迭代器
func newBtreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())
	saveValues := func(item btree.Item) bool {
		values[idx] = item.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

// 重新返回迭代器起点（第一个数据）
func (bti *btreeIterator) Rewind() {
	bti.curIndex = 0
}

// 查找第一个大于等于（或小于等于）目标的key，从此开始遍历
func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) <= 0
		})
	} else {
		bti.curIndex = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(bti.values[i].key, key) >= 0
		})
	}
}

// 跳转到下一个key
func (bti *btreeIterator) Next() {
	bti.curIndex++
}

// 是否遍历完所有key
func (bti *btreeIterator) Valid() bool {
	return bti.curIndex < len(bti.values)
}

// 当前位置的key值
func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curIndex].key
}

// 当前位置的value值
func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curIndex].pos
}

// 关闭迭代器，释放对应资源
func (bti *btreeIterator) Close() {
	bti.values = nil
}
