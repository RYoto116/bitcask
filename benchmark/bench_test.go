package benchmark

import (
	"bitcask"
	"bitcask/utils"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var db *bitcask.DB

func init() {
	opt := bitcask.DefaultOptions
	opt.DirPath, _ = os.MkdirTemp("", "bitcask-go-bench")

	var err error
	db, err = bitcask.OpenDB(opt)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

func Benchmark_Put(b *testing.B) {
	// 重置计时器，输出更精确的测试结果
	b.ResetTimer()
	// 打印内存分配情况
	b.ReportAllocs()

	// 在循环中进行测试
	for i := 0; i < b.N; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024)) // 1 KB大小的value
		assert.Nil(b, err)
	}
}

func Benchmark_Get(b *testing.B) {
	for i := 0; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024)) // 1 KB大小的value
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(utils.GetTestKey(rand.Int()))
		if err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}

func Benchmark_Delete(b *testing.B) {
	for i := 0; i < 5000; i++ {
		err := db.Put(utils.GetTestKey(i), utils.RandomValue(1024)) // 1 KB大小的value
		assert.Nil(b, err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := db.Delete(utils.GetTestKey(rand.Int())); err != nil && err != bitcask.ErrKeyNotFound {
			b.Fatal(err)
		}
	}
}
