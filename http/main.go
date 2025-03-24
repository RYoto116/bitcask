package main

import (
	"bitcask"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
)

var db *bitcask.DB

// 初始化 DB 实例s
func init() {
	var err error
	opt := bitcask.DefaultOptions
	opt.DirPath, err = os.MkdirTemp("", "bitcask-go-http")
	db, err = bitcask.OpenDB(opt)
	if err != nil {
		panic(fmt.Sprintf("failed to open db: %v", err))
	}
}

// ResponseWriter 接口类型，用于向客户端发送响应
// Request HTTP 请求对象，包含HTTP报文的URL、首部等全部信息
func handlePut(writer http.ResponseWriter, req *http.Request) {

	// 请求方式不是 Put，返回405状态码
	if req.Method != http.MethodPost {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 利用json解析POST请求中的数据
	var data map[string]string
	if err := json.NewDecoder(req.Body).Decode(&data); err != nil {
		http.Error(writer, err.Error(), http.StatusBadRequest) // 400 状态码，表示客户端请求报文有误
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError) // 500 状态码，表示服务端请求报文有误
			log.Printf("failed to put value in db: %v", err)
			return
		}
	}
}

func handleGet(writer http.ResponseWriter, req *http.Request) {

	// 请求方式不是 Get，返回405状态码
	if req.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := req.URL.Query().Get("key")
	value, err := db.Get([]byte(key))
	if err != nil && err != bitcask.ErrKeyNotFound {
		http.Error(writer, err.Error(), http.StatusInternalServerError) // 500 状态码，表示服务端请求报文有误
		log.Printf("failed to get value from db: %v", err)
		return
	}

	// 设置当前内容的MIME类型。MIME 是描述消息内容类型的标准，用来表示文档、文件或字节流的性质和格式。
	// 通用结构：type/subtype。type=application 表明是某种二进制数据
	writer.Header().Set("Content-Type", "application/json")
	// 通过 ResponseWriter 直接向客户端返回JSON数据
	// Encode 方法会将 value 编码为一个 JSON 字符串
	_ = json.NewEncoder(writer).Encode(string(value)) // 注意将[]byte转换为string类型，否则value会乱码
}

func handleDelete(writer http.ResponseWriter, req *http.Request) {
	// Delete请求方式
	if req.Method != http.MethodDelete {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := req.URL.Query().Get("key")
	if err := db.Delete([]byte(key)); err != nil {
		http.Error(writer, err.Error(), http.StatusInternalServerError) // 500 状态码，表示服务端请求报文有误
		log.Printf("failed to delete value in db: %v", err)
		return
	}

	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode("OK")
}

func handleListKeys(writer http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	keys := db.ListKeys()
	writer.Header().Set("Content-Type", "application/json")

	var result []string
	for _, key := range keys {
		result = append(result, string(key))
	}
	_ = json.NewEncoder(writer).Encode(result)
}

func handleStat(writer http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodGet {
		http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stat := db.Stat()
	writer.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(writer).Encode(stat)
}

func main() {
	// HandleFunc 注册路径处理函数
	http.HandleFunc("/bitcask/put", handlePut)
	http.HandleFunc("/bitcask/get", handleGet)
	http.HandleFunc("/bitcask/delete", handleDelete)
	http.HandleFunc("/bitcask/listkeys", handleListKeys)
	http.HandleFunc("/bitcask/stat", handleStat)

	// 创建 Server 类型对象，启动 HTTP 服务，在指定socket监听请求
	_ = http.ListenAndServe("localhost:8080", nil)
}
