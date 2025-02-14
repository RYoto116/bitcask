package utils

import (
	"fmt"
	"math/rand"
	"time"
)

var (
	randNum = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("bitcask-key-%09d", i))
}

func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[randNum.Intn(len(letters))]
	}
	return []byte("bitcask-value-" + string(b))
}
