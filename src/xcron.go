package main

import (
	"engine"
	"flag"
	"fmt"
	"global"
	"log"
	"server"
)

var (
	configFile = flag.String("conf", "./xcron.toml.master", "Path to the configuration filename")
)

func main() {
	flag.Parse()
	//init config
	if err := global.InitConfig(*configFile); err != nil {
		log.Panicf("init config err:%s", err.Error())
	}
	log.Printf("config: %s", global.XcronConf.String())
	//engine
	dir := global.XcronConf.RaftDir
	addr := fmt.Sprintf("%s:%s", global.XcronState.GetLocalAddr(), global.XcronConf.PeerPort)
	xengine, err := engine.NewEngine(dir, addr)
	if err != nil {
		log.Panicf("engine NewEngine err:%s", err.Error())
	}
	if err := xengine.Open(); err != nil {
		log.Panicf("engine open err:%s", err.Error())
	}
	defer xengine.Close()
	//run http...
	httpServer := server.NewHttpServer(global.XcronConf.HttpPort, xengine)
	httpServer.Run()
	// rpc...
}
