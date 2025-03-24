package index

import (
	"bitcask/data"
	"bytes"
	"sort"
	"sync"

	goart "github.com/plar/go-adaptive-radix-tree/v2"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) *data.LogRecordPos {
	art.lock.Lock()
	oldValue, update := art.tree.Insert(key, pos)
	art.lock.Unlock()
	if !update {
		return nil
	}

	return oldValue.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	pos, ok := art.tree.Search(key)
	if !ok {
		return nil
	}
	return pos.(*data.LogRecordPos)
}

func (art *AdaptiveRadixTree) Delete(key []byte) (*data.LogRecordPos, bool) {
	art.lock.Lock()
	oldValue, deleted := art.tree.Delete(key)
	art.lock.Unlock()

	if !deleted {
		return nil, false
	}
	return oldValue.(*data.LogRecordPos), true
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	if art.tree == nil {
		return nil
	}

	art.lock.RLock()
	defer art.lock.RUnlock()

	return newARTIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Close() error {
	return nil
}

type artIterator struct {
	curIndex int     // 当前迭代位置
	reverse  bool    // 是否反向遍历
	values   []*Item // key+位置信息
}

func newARTIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int = 0
	values := make([]*Item, tree.Size())

	if reverse {
		idx = tree.Size() - 1
	}

	saveValue := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}

		values[idx] = item

		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}

	tree.ForEach(saveValue)

	return &artIterator{
		curIndex: 0,
		reverse:  reverse,
		values:   values,
	}
}

func (arti *artIterator) Rewind() {
	arti.curIndex = 0
}

// 查找第一个大于等于（或小于等于）目标的key，从此开始遍历
func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.curIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) <= 0
		})
	} else {
		arti.curIndex = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(arti.values[i].key, key) >= 0
		})
	}
}

// 跳转到下一个key
func (arti *artIterator) Next() {
	arti.curIndex++
}

// 是否遍历完所有key
func (arti *artIterator) Valid() bool {
	return arti.curIndex < len(arti.values)
}

// 当前位置的key值
func (arti *artIterator) Key() []byte {
	return arti.values[arti.curIndex].key
}

// 当前位置的value值
func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.curIndex].pos
}

// 关闭迭代器，释放对应资源
func (arti *artIterator) Close() {
	arti.values = nil
}
