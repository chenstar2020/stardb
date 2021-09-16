package main

import (
	"stardb"
	"flag"
	"fmt"
	"github.com/pelletier/go-toml"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"stardb/cmd"
)

func init(){
	banner, _ := ioutil.ReadFile("../../resource/banner.txt")
	fmt.Println(string(banner))
}

var config = flag.String("config", "", "the config for stardb")

var dirPath = flag.String("dir_path", "", "the dirpath for the database")

func main(){
	flag.Parse()
	fmt.Println("config:", *config, "dir_path:", *dirPath)

	var cfg stardb.Config
	if *config == ""{
		log.Println("no config set, using the default config.")
		cfg = stardb.DefaultConfig()
	} else {
		c, err := newConfigFromFile(*config)
		if err != nil{
			log.Printf("load config err: %v", err)
			return
		}
		cfg = *c
	}

	if *dirPath == ""{
		log.Println("no dir path set, use os tmp dir")
	}else{
		cfg.DirPath = *dirPath
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill,
		syscall.SIGHUP,  //连接断开
		syscall.SIGINT,  //终端中断符
		syscall.SIGTERM, //终止
		syscall.SIGQUIT, //终端退出符
	)

	server, err := cmd.NewServer(cfg)
	if err != nil{
		log.Printf("create stardb server err: %+v\n", err)
		return
	}

	go server.Listen(cfg.Addr)

	<-sig
	server.Stop()
	log.Println("stardb is ready to exist, bye...")
}


func newConfigFromFile(config string)(*stardb.Config, error){
	data, err := ioutil.ReadFile(config)
	if err != nil{
		return nil, err
	}
	var cfg = new(stardb.Config)
	err = toml.Unmarshal(data, cfg)
	if err != nil{
		return nil, err
	}
	return cfg, nil
}