package main

import (
	"flag"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"github.com/peterh/liner"
	"log"
	"os"
	"strings"
)

var commandList = [][]string{
	{"SET", "key value", "STRING"},
	{"GET", "key", "STRING"},
	{"SETNX", "key value", "STRING"},
	{"GETSET", "key value", "STRING"},
	{"APPEND", "key value", "STRING"},
	{"STRLEN", "key", "STRING"},
	{"STREXISTS", "key", "STRING"},
	{"STRREM", "key", "STRING"},
	{"PREFIXSCAN", "prefix limit offset", "STRING"},
	{"RANGESCAN", "start end", "STRING"},
	{"EXPIRE", "key seconds", "STRING"},
	{"PERSIST", "key", "STRING"},
	{"TTL", "key", "STRING"},

	{"LPUSH", "key value [value...]", "LIST"},
	{"RPUSH", "key value [value...]", "LIST"},
	{"LPOP", "key", "LIST"},
	{"RPOP", "key", "LIST"},
	{"LINDEX", "key index", "LIST"},
	{"LREM", "key value count", "LIST"},
	{"LINSERT", "key BEFORE|AFTER pivot element", "LIST"},
	{"LSET", "key index value", "LIST"},
	{"LTRIM", "key start end", "LIST"},
	{"LRANGE", "key start end", "LIST"},
	{"LLEN", "key", "LIST"},
	{"LKEYEXISTS", "key", "LIST"},
	{"LVALEXISTS", "key value", "LIST"},

	{"HSET", "key field value", "HASH"},
	{"HSETNX", "key field value", "HASH"},
	{"HGET", "key field", "HASH"},
	{"HGETALL", "key", "HASH"},
	{"HDEL", "key field [field...]", "HASH"},
	{"HEXISTS", "key field", "HASH"},
	{"HLEN", "key", "HASH"},
	{"HKEYS", "key", "HASH"},
	{"HVALS", "key", "HASH"},

	{"SADD", "key members [members...]", "SET"},
	{"SPOP", "key count", "SET"},
	{"SISMEMBER", "key member", "SET"},
	{"SRANDMEMBER", "key count", "SET"},
	{"SREM", "key members [members...]", "SET"},
	{"SMOVE", "src dst member", "SET"},
	{"SCARD", "key", "key", "SET"},
	{"SMEMBERS", "key", "SET"},
	{"SUNION", "key [key...]", "SET"},
	{"SDIFF", "key [key...]", "SET"},

	{"ZADD", "key score member", "ZSET"},
	{"ZSCORE", "key member", "ZSET"},
	{"ZCARD", "key", "ZSET"},
	{"ZRANK", "key member", "ZSET"},
	{"ZREVRANK", "key member", "ZSET"},
	{"ZREM", "key member", "ZSET"},
	{"ZGETBYRANK", "key rank", "ZSET"},
	{"ZREVGETBYRANk", "key rank", "ZSET"},
	{"ZSCORERANGE", "key min max", "ZSET"},
	{"ZREVSCORERANGE", "key max min", "ZSET"},
}

var host = flag.String("h", "127.0.0.1", "the stardb server host, default 127.0.0.1")
var port = flag.Int("p", 5200, "the stardb server port, default 5200")

const cmdHistoryPath = "D:\\github\\stardb\\clientDir\\historyCmd.txt"

const (
	help = "help"
	quit = "quit"
	exit = "exit"
)

func main(){
	flag.Parse()

	addr := fmt.Sprintf("%s:%d", *host, *port)
	conn, err := redis.Dial("tcp", addr)
	if err != nil{
		log.Println("tcp dial err: ", err)
		return
	}

	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)
	//设置比较函数 从命令表中返回前缀相同的命令
	line.SetCompleter(func(line string) (res []string) {
		for _, c := range commandList {
			if strings.HasPrefix(c[0], strings.ToLower(line)) {
				res = append(res, strings.ToLower(c[0]))
			}
		}
		return
	})

	//打开历史命令文件，加载历史命令
	if f, err := os.Open(cmdHistoryPath); err == nil{
		line.ReadHistory(f)
		f.Close()
	}
	//保存历史命令
	defer func() {
		if f, err := os.Create(cmdHistoryPath); err != nil{
			fmt.Printf("writing cmd history err:%v\n", err)
		}else{
			line.WriteHistory(f)
			line.Close()
		}
	}()

	commandSet := map[string]bool{}
	for _, cmd := range commandList{
		commandSet[strings.ToLower(cmd[0])] = true
	}

	prompt := addr + ">"

	for{
		cmd, err := line.Prompt(prompt)  //前缀显示127.0.0.1>
		if err != nil{
			fmt.Println(err)
			break
		}

		cmd = strings.TrimSpace(cmd)
		if len(cmd) == 0 {
			continue
		}

		c := strings.Split(cmd, " ")
		if len(c[0]) == 1 && strings.ToLower(c[0]) == help{
			printCmdHelp()
		}else if strings.ToLower(c[0]) == quit || strings.ToLower(c[0]) == exit{
			break
		}else if len(c) == 2 && strings.ToLower(c[0]) == help { //打印命令帮助信息
			helpCmd := strings.ToLower(c[1])
			if !commandSet[helpCmd]{
				fmt.Printf("(error) ERR unknown command '%v'\n", helpCmd)
				continue
			}

			for _, command := range commandList{
				if strings.ToLower(command[0]) == helpCmd{
					fmt.Println()
					fmt.Println("--usage: " + helpCmd + " " + command[1])
					fmt.Println("--group: " + command[2] + "\n")
				}
			}
		}else{
			//执行命令和回显
			line.AppendHistory(cmd)

			lowerC := strings.ToLower(strings.TrimSpace(c[0]))
			if !commandSet[lowerC] {
				fmt.Printf("(error) ERR unknown command '%v'\n", lowerC)
				continue
			}

			command, args := parseCommandLine(cmd)
			rawResp, err := conn.Do(command, args...)
			if err != nil{
				fmt.Printf("(error) %v \n", err)
				continue
			}
			switch reply := rawResp.(type){
			case []byte:
				println(string(reply))
			case string:
				println(reply)
			case nil:
				println("(nil)")
			case redis.Error:
				fmt.Printf("(error) %v \n", reply)
			case int64:
				fmt.Printf("(integer) %d \n", reply)
			case []interface{}:
				for i, e := range reply{
					switch element := e.(type){
					case string:
						fmt.Printf("%d) %s\n", i+1, element)
					case []byte:
						fmt.Printf("%d) %s\n", i+1, string(element))
					default:
						fmt.Printf("%d) %v\n", i+1, element)
					}
			}
			}
		}
	}
}

func printCmdHelp(){
	help := `
 Thanks for using StarDB
 stardb-cli
 To get help about command:
	Type: "help <command>" for help on command
 To quit:
	<ctrl+c> or <quit> or <exit>`
	fmt.Println(help)
}

func parseCommandLine(cmdLine string)(string, []interface{}){
	arr := strings.Split(cmdLine, " ")
	if len(arr) == 0{
		return "", nil
	}
	args := make([]interface{}, 0)
	for i := 1; i < len(arr); i++ {
		args = append(args, arr[i])
	}
	return arr[0], args
}