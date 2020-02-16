package trigger

import (
	"fmt"
	"github.com/alwaysthanks/xcron/core/engine"
	"github.com/alwaysthanks/xcron/core/entity"
	"github.com/alwaysthanks/xcron/core/global"
	"github.com/ouqiang/timewheel"
	"github.com/robfig/cron"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

var trig *Trigger

func Init() error {
	trig = NewTrigger()
	return nil
}

func SetInstanceTask(taskId string, timestamp int64) (timerId string, err error) {
	return trig.SetInstanceTask(taskId, timestamp)
}

func SetCrontabTask(taskId string, crontab string) (timerId string, err error) {
	return trig.SetCrontabTask(taskId, crontab)
}

func Close() error {
	trig.Stop()
	return nil
}

type Trigger struct {
	logger *log.Logger
	//tw for immediate task
	tw *timewheel.TimeWheel
	//cron for crontab task
	cron *cron.Cron
}

func NewTrigger() *Trigger {
	logger := log.New(os.Stderr, "[xcron-trigger] ", log.LstdFlags)
	trig := &Trigger{
		logger: logger,
	}
	//time wheel
	tw := timewheel.New(time.Second, 3600, func(reqTaskId interface{}) {
		taskId := reqTaskId.(string)
		if err := trig.runTask(taskId); err != nil {
			trig.logger.Printf("[error][trig] timewheel run task error. taskId:%s, err:%s", taskId, err.Error())
			atomic.AddInt64(&global.XcronState.InstanceFailedTaskCount, 1)
			return
		}
		atomic.AddInt64(&global.XcronState.InstanceCompleteTaskCount, 1)
	})
	tw.Start()
	trig.tw = tw
	//crontab
	cr := cron.New(cron.WithChain(cron.Recover(cron.DefaultLogger)))
	cr.Start()
	trig.cron = cr
	return trig
}

//add instance timer
func (trig *Trigger) SetInstanceTask(taskId string, timestamp int64) (timerId string, err error) {
	duration := timestamp - time.Now().Unix()
	trig.tw.AddTimer(time.Duration(duration)*time.Second, taskId, taskId)
	atomic.AddInt64(&global.XcronState.InstanceAddTaskCount, 1)
	return taskId, nil
}

//add crontab timer
func (trig *Trigger) SetCrontabTask(taskId string, crontab string) (timerId string, err error) {
	cronId, _ := trig.cron.AddFunc(crontab, func() {
		if err := trig.runTask(taskId); err != nil {
			atomic.AddInt64(&global.XcronState.CrontabFailedTaskCount, 1)
			trig.logger.Printf("[error][trig] crontab run task error. taskId:%s, err:%s", taskId, err.Error())
			return
		}
		atomic.AddInt64(&global.XcronState.CrontabRunTaskCount, 1)
	})
	atomic.AddInt64(&global.XcronState.CrontabAddTaskCount, 1)
	timerId = strconv.Itoa(int(cronId))
	return timerId, nil
}

//todo
//del timer

//dispatcher stop
func (trig *Trigger) Stop() {
	trig.tw.Stop()
	trig.cron.Stop()
}

//running task
func (trig *Trigger) runTask(taskId string) error {
	var err error
	var task *entity.XcronTask
	if task, err = trig.preTask(taskId); err != nil {
		trig.logger.Printf("[error][trig.runTask] preTask error. taskId:%s,err:%s", taskId, err.Error())
		return err
	}
	if err = task.Callback.Post(); err != nil {
		trig.logger.Printf("[error][trig.runTask] callback error. taskId:%s,err:%s", taskId, err.Error())
		return err
	}
	if _, err = trig.afterTask(taskId); err != nil {
		trig.logger.Printf("[error][trig.runTask] afterTask error. taskId:%s,err:%s", taskId, err.Error())
		return err
	}
	return nil
}

//preTask for dispatcher previous
func (trig *Trigger) preTask(taskId string) (task *entity.XcronTask, err error) {
	task, err = entity.GetTask(taskId)
	if err != nil {
		trig.logger.Printf("[error][trig.preTask] getTask error. taskId:%s,err:%s", taskId, err.Error())
		return nil, err
	}
	//before.atomic increment
	key := fmt.Sprintf("task_id:%s;run_times:%d;state:%d", taskId, task.RunTimes, entity.TaskStatePrev)
	counter, err := engine.AtomCounter(key)
	if err != nil {
		trig.logger.Printf("[error][trig.preTask] atomCounter error.key:%s,err:%s", key, err.Error())
		return nil, err
	}
	if counter != 1 {
		trig.logger.Printf("[info][trig.preTask] atomCounter concurrent.key:%s,counter:%d", key, counter)
		return nil, fmt.Errorf("concurrency run task")
	}
	task.State = entity.TaskStatePrev
	if err := entity.SetTask(task); err != nil {
		trig.logger.Printf("[info][trig.preTask] setTask error. taskId:%s,err:%s", taskId, err.Error())
		return nil, err
	}
	return task, nil
}

//afterTask for dispatcher after
func (trig *Trigger) afterTask(taskId string) (task *entity.XcronTask, err error) {
	task, err = entity.GetTask(taskId)
	if err != nil {
		trig.logger.Printf("[error][trig.afterTask] getTask error. taskId:%s,err:%s", taskId, err.Error())
		return nil, err
	}
	task.State = entity.TaskStateAfter
	//after.RunCount++
	task.RunTimes++
	if err := entity.SetTask(task); err != nil {
		trig.logger.Printf("[info][trig.afterTask] setTask error. taskId:%s,err:%s", taskId, err.Error())
		return nil, err
	}
	return task, nil
}
