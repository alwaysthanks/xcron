package global

import (
	"lib/network"
	"log"
)

const (
	HashDataShard = 10240
)

var XcronState *state

func init() {
	localNetAddr, err := network.GetDefaultInternalIp()
	if err != nil {
		log.Panicf("local net addr err:%s", err.Error())
	}
	log.Printf("local addr:%s", localNetAddr)
	XcronState = &state{
		localNetWorkAddr: localNetAddr,
		groupDataShard:   HashDataShard,
	}
}

type state struct {
	localNetWorkAddr string
	isMachineActive  bool  //is machine working normally
	groupDataShard   int32 //group data sharding param

	//store task count
	StoreInstanceTaskCount int64
	StoreCrontabTaskCount  int64
	//add task count
	InstanceAddTaskCount int64
	CrontabAddTaskCount  int64
	//run task count
	CrontabRunTaskCount       int64
	InstanceCompleteTaskCount int64
	//failed task count
	CrontabFailedTaskCount  int64
	InstanceFailedTaskCount int64
	//instance retry
	InstanceRetryTaskCount int64
}

func (s *state) GetDataShard() int32 {
	return s.groupDataShard
}

func (s *state) ActiveMachine() {
	s.isMachineActive = true
}

func (s *state) DeActiveMachine() {
	s.isMachineActive = false
}

func (s *state) GetLocalAddr() string {
	return s.localNetWorkAddr
}
