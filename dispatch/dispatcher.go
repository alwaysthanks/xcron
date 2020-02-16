package dispatch

import (
	"context"
	"fmt"
	"github.com/alwaysthanks/xcron/core/engine"
	"github.com/alwaysthanks/xcron/core/entity"
	"github.com/alwaysthanks/xcron/core/global"
	"github.com/alwaysthanks/xcron/core/lib/json"
	"github.com/alwaysthanks/xcron/core/lib/routine"
	"github.com/alwaysthanks/xcron/trigger"
	"github.com/ouqiang/timewheel"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	//use planB machine for task process when priority machine downtime accidentally
	PlanBSleepInterval = time.Second * 5
	//when server start, db data need recover first.
	DispatcherStartWork = time.Second * 10
	//parallel process queue data
	ParallelRoutineCount = 3
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
		dispatcher.logger.Printf("[info][dispatcher] planB timewheel task run.taskId:%s,planB host:%s", taskId, dispatcher.localAddr)
		//for planB task invoke
		if err := dispatcher.invokeTask(taskId); err != nil {
			dispatcher.logger.Printf("[error][dispatcher] planB timewheel run planB task error. taskId:%s, err:%s", taskId, err.Error())
		}
	})
	tw.Start()
	dispatcher.tw = tw
	return dispatcher
}

//Loop for consume task data.
func (dispatcher *Dispatcher) Loop() {
	consumeFunc := func(data []byte) error {
		var task = &entity.XcronTask{}
		if err := json.Unmarshal(data, task); err != nil {
			dispatcher.logger.Printf("[error][Dispatcher.Loop] unmarsh task error. err:%s", err.Error())
			return err
		}
		//set data
		if err := engine.SetData(task.TaskId, data); err != nil {
			dispatcher.logger.Printf("[error][Dispatcher.Loop] engine setData for task error. taskId:%s,err:%s", task.TaskId, err.Error())
			return err
		}
		//distribute
		if err := dispatcher.distributeTask(task); err != nil {
			dispatcher.logger.Printf("[error][Dispatcher.Loop] distribute task error. taskId:%s,err:%s", task.TaskId, err.Error())
			return err
		}
		return nil
	}
	//Loop for
	pool := routine.NewRoutinePool(ParallelRoutineCount)
	for {
		//parallel go routine process
		pool.Start(context.Background(), func(t *routine.Task) error {
			data, err := engine.DeQueue(consumeFunc)
			if err != nil {
				dispatcher.logger.Printf("[error][Dispatcher.Loop] consume queue error. err:%s", err.Error())
				return err
			}
			dispatcher.logger.Printf("[info][Dispatcher.Loop] consume queue success.data:%s", string(data))
			return nil
		})
	}

}

//distributeTask to ensure which machine should invoke task.
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

//invokeTask to invoke task in current work machine.
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
