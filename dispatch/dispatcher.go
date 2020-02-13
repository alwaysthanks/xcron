package dispatch

import (
	"fmt"
	"github.com/alwaysthanks/xcron/core/engine"
	"github.com/alwaysthanks/xcron/core/entity"
	"github.com/alwaysthanks/xcron/core/global"
	"github.com/alwaysthanks/xcron/core/lib/json"
	"github.com/alwaysthanks/xcron/trigger"
	"github.com/grandecola/bigqueue"
	"github.com/ouqiang/timewheel"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	EmptySleepInterval  = time.Millisecond * 200 // 200ms
	PlanBSleepInterval  = time.Second * 5
	DispatcherStartWork = time.Second * 5
)

func Init() error {
	dispatcher := NewDispatcher()
	go func() {
		//sleep for current machine data ready
		time.Sleep(DispatcherStartWork)
		dispatcher.Loop()
	}()
	return nil
}

type Dispatcher struct {
	logger *log.Logger
	//tw for planB task
	tw *timewheel.TimeWheel
	//local Addr
	localAddr string
}

func NewDispatcher() *Dispatcher {
	logger := log.New(os.Stderr, "[xcron-dispatcher] ", log.LstdFlags)
	dispatcher := &Dispatcher{
		logger:    logger,
		localAddr: global.XcronState.GetLocalAddr(),
	}
	//time wheel
	tw := timewheel.New(time.Second, 3600, func(reqTaskId interface{}) {
		taskId := reqTaskId.(string)
		if err := dispatcher.invokeTask(taskId); err != nil {
			dispatcher.logger.Printf("[error][dispatcher] timewheel run planB task error. taskId:%s, err:%s", taskId, err.Error())
		}
		dispatcher.logger.Printf("[info][dispatcher] timewheel task run in planB.taskId:%s, planB host:%s", taskId, dispatcher.localAddr)
	})
	tw.Start()
	dispatcher.tw = tw
	return dispatcher
}

func (dispatcher *Dispatcher) Loop() {
	for {
		data, err := engine.DeQueue()
		if err != nil {
			if err == bigqueue.ErrEmptyQueue {
				time.Sleep(EmptySleepInterval)
			} else {
				dispatcher.logger.Printf("[error][Dispatcher.Loop] dequeue error. err:%s", err.Error())
			}
			continue
		}
		var task = &entity.XcronTask{}
		if err := json.Unmarshal(data, task); err != nil {
			dispatcher.logger.Printf("[error][Dispatcher.Loop] unmarsh task error. err:%s", err.Error())
			continue
		}
		//retry 3 times
		var flag bool
		for i := 1; i <= 3; i++ {
			if err := engine.SetData(task.TaskId, data); err != nil {
				dispatcher.logger.Printf("[error][Dispatcher.Loop][%d] engine setData for task error. taskId:%s,err:%s", i, task.TaskId, err.Error())
			} else {
				flag = true
				break
			}
		}
		if !flag {
			dispatcher.logger.Printf("[error][Dispatcher.Loop] engine setData for task retry all failed. taskId:%s", task.TaskId)
			continue
		}
		//distribute
		if err := dispatcher.distributeTask(task); err != nil {
			dispatcher.logger.Printf("[error][Dispatcher.Loop] distribute task error. taskId:%s,err:%s", task.TaskId, err.Error())
		}
	}
}

func (dispatcher *Dispatcher) distributeTask(task *entity.XcronTask) error {
	//current serial number
	activePeersCount := engine.GetActivePeersCount()
	//serial mod number
	serialNumber := engine.GetActivePeerIndex(dispatcher.localAddr) - 1
	planBNumber := serialNumber - 1
	if planBNumber < 0 {
		planBNumber = activePeersCount - 1
	}
	if serialNumber < 0 || activePeersCount == 0 {
		dispatcher.logger.Printf("[error][dispatcher.distributeTask] get serial number error. activePeersCount:%d,serialNumber:%d", activePeersCount, serialNumber)
		return fmt.Errorf("serial count error")
	}
	mod := task.TaskHash % activePeersCount
	dispatcher.logger.Printf("[info][dispatcher.distributeTask] task begin dispatch. cur machine info:host:%s,activePeersCount:%d,serialNumber:%d, mod:%d", dispatcher.localAddr, activePeersCount, serialNumber, mod)
	//if only one machine, not planB
	if serialNumber != 0 && mod == planBNumber {
		//double check for planB
		dispatcher.logger.Printf("[info][dispatcher.distributeTask] check for planB task. taskId:%s,host:%s", task.TaskId, dispatcher.localAddr)
		key := fmt.Sprintf("%s:%s", task.TaskId, dispatcher.localAddr)
		dispatcher.tw.AddTimer(PlanBSleepInterval, key, task.TaskId)
		return nil
	} else if mod != serialNumber {
		dispatcher.logger.Printf("[info][dispatcher.distributeTask] not this machine for work. host:%s, activePeersCount:%d,serialNumber:%d, mod:%d", dispatcher.localAddr, activePeersCount, serialNumber, mod)
		return nil
	}
	//invoke
	return dispatcher.invokeTask(task.TaskId)
}

func (dispatcher *Dispatcher) invokeTask(taskId string) error {
	task, err := entity.GetTask(taskId)
	if err != nil {
		dispatcher.logger.Printf("[error][dispatcher.invokeTask] GetTask error. taskId:%s,err:%s", task.TaskId, err.Error())
		return err
	}
	if task.State != entity.TaskStateQueue {
		dispatcher.logger.Printf("[info][dispatcher.invokeTask] task state(%d) not queue. task has run normally.taskId:%s", task.State, task.TaskId)
		return fmt.Errorf("task state not queue")
	}
	//invoke
	var timerId string
	switch task.Type {
	case entity.TaskTypeInstance:
		timestamp, _ := strconv.ParseInt(task.Format, 10, 64)
		timerId, _ = trigger.SetInstanceTask(task.TaskId, timestamp)
	case entity.TaskTypeCrontab:
		timerId, _ = trigger.SetCrontabTask(task.TaskId, task.Format)
	default:
		return fmt.Errorf("not support task type")
	}
	task.TimerId = timerId
	task.State = entity.TaskStateSet
	task.Host = dispatcher.localAddr
	return entity.SetTask(task)
}
