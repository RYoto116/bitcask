package main

import (
	"bitcask"
	bitcask_redis "bitcask/redis"
	"log"
	"sync"

	"github.com/tidwall/redcon"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*bitcask_redis.RedisDataStructure
	server *redcon.Server
	mu     *sync.Mutex
}

func main() {
	// 打开redis数据结构服务
	rds, err := bitcask_redis.NewRedisDataStructure(bitcask.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*bitcask_redis.RedisDataStructure),
		mu:  new(sync.Mutex),
	}
	// 设置默认数据库
	bitcaskServer.dbs[0] = rds

	// 初始化redis服务器
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.closed) // handler: 处理客户端命令
	bitcaskServer.listen()
}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connections.")
	_ = svr.server.ListenAndServe() // CLOSER ==> LISTEN
}

// 处理新来的连接
func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()

	cli.server = svr
	cli.db = svr.dbs[0]  // ?
	conn.SetContext(cli) // 设置用户上下文
	return true
}

// 断开连接后的处理
func (srv *BitcaskServer) closed(conn redcon.Conn, err error) {
	for _, db := range srv.dbs {
		_ = db.Close()
	}

	_ = srv.server.Close()
}

// redis协议解析示例
// func main() {
// 	conn, err := net.Dial("tcp", "localhost:6379")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// 向redis发送命令
// 	cmd := "set k-name v-name1\r\n"
// 	conn.Write([]byte(cmd))

// 	// 解析redis响应
// 	reader := bufio.NewReader(conn)
// 	res, err := reader.ReadString('\n')
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Println(res)
// }
