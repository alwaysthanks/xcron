package dispatch

import (
	"bytes"
	"errors"
	"github.com/ouqiang/timewheel"
	"github.com/robfig/cron"
	"global"
	"lib/http"
	"lib/json"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	errHookIsInvalid = errors.New("hook is invalid")
)

const (
	TaskTypeInstance = 1
	TaskTypeCrontab  = 2
)

const (
	TaskStateStore = 0
	TaskStateSet   = 1
	TaskStatePrev  = 2
	TaskStateAfter = 3
)

type Hook func(taskId string) (task *XcronTask, err error)

type Dispatcher struct {
	logger *log.Logger
	//task running pre,after hook
	preHook   Hook
	afterHook Hook
	//tw for immediate task
	tw *timewheel.TimeWheel
	//cron for crontab task
	cron *cron.Cron
}

func NewDispatcher(logger *log.Logger, preHook, afterHook Hook) (*Dispatcher, error) {
	//hook
	if preHook == nil || afterHook == nil {
		return nil, errHookIsInvalid
	}
	dispatcher := &Dispatcher{
		logger:    logger,
		preHook:   preHook,
		afterHook: afterHook,
	}
	//time wheel
	tw := timewheel.New(time.Second, 3600, func(reqTaskId interface{}) {
		taskId := reqTaskId.(string)
		if err := dispatcher.runTask(taskId); err != nil {
			dispatcher.logger.Printf("[error] timewheel run task error. taskId:%s, err:%s", taskId, err.Error())
			atomic.AddInt64(&global.XcronState.InstanceFailedTaskCount, 1)
			return
		}
		atomic.AddInt64(&global.XcronState.InstanceCompleteTaskCount, 1)
	})
	tw.Start()
	//crontab
	cr := cron.New(cron.WithChain(cron.Recover(cron.DefaultLogger)))
	cr.Start()
	return &Dispatcher{tw: tw, cron: cr, preHook: preHook, afterHook: afterHook}, nil
}

//running task
func (dispatcher *Dispatcher) runTask(taskId string) error {
	var err error
	var task *XcronTask
	if task, err = dispatcher.preHook(taskId); err != nil {
		return err
	}
	if err = task.Callback.Post(); err != nil {
		return err
	}
	if _, err = dispatcher.afterHook(taskId); err != nil {
		return err
	}
	return nil
}

type XcronTask struct {
	TaskId     string    `json:"task_id"`   //task uuid
	TaskHash   int64     `json:"task_hash"` //task number for hash strategy
	TimerId    string    `json:"timer_id"`  // task run timerId, stop/delete timer
	Type       int16     `json:"type"`
	Host       string    `json:"host"` // which host
	State      int16     `json:"state"`
	Format     string    `json:"format"`
	RunTimes   int32     `json:"run_times"`
	Callback   *Callback `json:"callback"`
	CreateTime int64     `json:"create_time"`
}

func (task *XcronTask) Encode() []byte {
	data, _ := json.Marshal(task)
	return data
}

//add timer
func (dispatcher *Dispatcher) SetInstanceTask(taskId string, timestamp int64) (timerId string, err error) {
	duration := time.Now().Unix() - timestamp
	dispatcher.tw.AddTimer(time.Duration(duration)*time.Second, taskId, taskId)
	atomic.AddInt64(&global.XcronState.InstanceAddTaskCount, 1)
	return taskId, nil
}

//add timer
func (dispatcher *Dispatcher) SetCrontabTask(taskId string, crontab string) (timerId string, err error) {
	cronId, _ := dispatcher.cron.AddFunc(crontab, func() {
		if err := dispatcher.runTask(taskId); err != nil {
			atomic.AddInt64(&global.XcronState.CrontabFailedTaskCount, 1)
			dispatcher.logger.Printf("[error] crontab run task error. taskId:%s, err:%s", taskId, err.Error())
			return
		}
		atomic.AddInt64(&global.XcronState.CrontabRunTaskCount, 1)
	})
	atomic.AddInt64(&global.XcronState.CrontabAddTaskCount, 1)
	timerId = strconv.Itoa(int(cronId))
	return timerId, nil
}

//del timer

//stop
func (dispatcher *Dispatcher) Stop() {
	dispatcher.tw.Stop()
	dispatcher.cron.Stop()
}

type Callback struct {
	Url  string                 `json:"url"`
	Data map[string]interface{} `json:"data"`
}

var httpClient = http.NewHttpClient(3, time.Second*5)

func (call *Callback) Post() error {
	//todo
	//pool
	reqBody, _ := json.Marshal(call.Data)
	if _, err := httpClient.Post(call.Url, bytes.NewReader(reqBody)); err != nil {
		log.Printf("[error][Callback.Post] error. url:%s,body:%s,err:%s", call.Url, string(reqBody), err.Error())
		return err
	}
	return nil
}
