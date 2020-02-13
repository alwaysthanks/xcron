package entity

import (
	"bytes"
	"github.com/alwaysthanks/xcron/core/engine"
	"github.com/alwaysthanks/xcron/core/lib/http"
	"github.com/alwaysthanks/xcron/core/lib/json"
	"log"
	"time"
)

const (
	TaskTypeInstance = 1
	TaskTypeCrontab  = 2
)

const (
	TaskStateQueue = 0
	//TaskStateStore = 0
	TaskStateSet   = 1
	TaskStatePrev  = 2
	TaskStateAfter = 3
)

//get task
func GetTask(taskId string) (*XcronTask, error) {
	taskInfo, err := engine.GetData(taskId)
	if err != nil {
		return nil, err
	}
	var task = XcronTask{}
	if err := json.Unmarshal(taskInfo, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

//set task
func SetTask(task *XcronTask) error {
	data, _ := json.Marshal(task)
	return engine.SetData(task.TaskId, data)
}

//xcron task entity
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

type Callback struct {
	Url  string                 `json:"url"`
	Data map[string]interface{} `json:"data"`
}

var (
	//callback http client
	httpClient = http.NewHttpClient(3, time.Second*3)
)

func (call *Callback) Post() error {
	reqBody, _ := json.Marshal(call.Data)
	if _, err := httpClient.Post(call.Url, bytes.NewReader(reqBody)); err != nil {
		log.Printf("[error][Callback.Post] error. url:%s,body:%s,err:%s", call.Url, string(reqBody), err.Error())
		return err
	}
	return nil
}
