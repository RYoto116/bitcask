package main

import (
	"bitcask"
	bitcask_redis "bitcask/redis"
	"bitcask/utils"
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("wrong number of arguments for '%s' command", cmd)
}

type cmdHandler = func(cli *BitcaskClient, args [][]byte) (interface{}, error)

// 支持的命令, cmd ==> handler 映射
var supportedCommands = map[string]cmdHandler{
	"set":   set,
	"get":   get,
	"hset":  hset,
	"sadd":  sadd,
	"lpush": lpush,
	"zadd":  zadd,
}

type BitcaskClient struct {
	// 当前数据库实例
	db *bitcask_redis.RedisDataStructure

	// redis server
	server *BitcaskServer
}

// 处理客户端命令
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0])) // 命令类型
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command: '" + command + "'")
		return
	}

	// 取出客户端信息
	client, _ := conn.Context().(*BitcaskClient)
	// switch处理简单命令
	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("PONG")
	default:
		res, err := cmdFunc(client, cmd.Args[1:]) // 执行对应的cmdFunc
		if err != nil {
			if err == bitcask.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
		} else {
			conn.WriteAny(res)
		}
	}
}

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, val := args[0], args[1]
	if err := cli.db.Set(key, 0, val); err != nil {
		return nil, err
	}

	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}

	key := args[0]
	val, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	key, field, value := args[0], args[1], args[2]

	var ok = 0
	res, err := cli.db.HSet(key, field, value)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hset")
	}

	key, member := args[0], args[1]

	var ok = 0
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, member := args[0], args[1]

	size, err := cli.db.LPush(key, member)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(size), nil
}

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	key, score, member := args[0], args[1], args[2]

	var ok = 0
	res, err := cli.db.ZAdd(key, utils.FloatFromBytes(score), member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}
