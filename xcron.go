package main

import (
	"flag"
	"fmt"
	"github.com/alwaysthanks/xcron/core/engine"
	"github.com/alwaysthanks/xcron/core/global"
	"github.com/alwaysthanks/xcron/dispatch"
	"github.com/alwaysthanks/xcron/server"
	"github.com/alwaysthanks/xcron/trigger"
	"log"
)

var (
	configFile = flag.String("conf", "./xcron.toml", "Path to the config filename")
)

func main() {
	flag.Parse()
	//config init
	if err := global.InitConfig(*configFile); err != nil {
		log.Panicf("init config err:%s", err.Error())
	}
	log.Printf("config: %s", global.XcronConf.String())
	//engine init
	dir := global.XcronConf.RaftDir
	addr := fmt.Sprintf("%s:%d", global.XcronState.GetLocalAddr(), global.XcronConf.PeerPort)
	if err := engine.Init(dir, addr); err != nil {
		log.Panicf("engine Init err:%s", err.Error())
	}
	defer engine.Close()
	//trigger init
	if err := trigger.Init(); err != nil {
		log.Panicf("trigger init err:%s", err.Error())
	}
	defer trigger.Close()
	//dispatcher init
	if err := dispatch.Init(); err != nil {
		log.Panicf("dispatcher init err:%s", err.Error())
	}
	//run http...
	httpServer := server.NewHttpServer(global.XcronConf.HttpPort)
	httpServer.Run()
	// rpc...
}
