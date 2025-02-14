package main

import (
	"bitcask-kv"
	"fmt"
)

func main() {
	opt := bitcask.DefaultOptions
	opt.DirPath = "/tmp/bitcask_data"
	db, err := bitcask.OpenDB(opt)
	if err != nil {
		panic(err)
	}

	err = db.Put([]byte("name"), []byte("yjx"))
	if err != nil {
		panic(err)
	}
	err = db.Put([]byte("height"), []byte("178.5"))
	if err != nil {
		panic(err)
	}

	h, err := db.Get([]byte("height"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(h))

	err = db.Delete([]byte("height"))
	if err != nil {
		panic(err)
	}

	_, err = db.Get([]byte("height"))
	if err != nil {
		panic(err)
	}
}
