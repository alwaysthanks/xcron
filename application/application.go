package application

import (
	"errors"
	"github.com/alwaysthanks/xcron/core/engine"
	"github.com/alwaysthanks/xcron/core/entity"
	"github.com/alwaysthanks/xcron/core/global"
	"github.com/alwaysthanks/xcron/core/lib/json"
	"github.com/alwaysthanks/xcron/core/lib/snowflake"
	"github.com/robfig/cron"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	errTaskTimeInvalid     = errors.New("instance task time invalid")
	errTaskCronInvalid     = errors.New("crontab task invalid")
	errTaskInstanceEnQueue = errors.New("instance task enqueue error")
	errTaskCrontabEnQueue  = errors.New("crontab task enqueue error")
)

//add instance task
func AddInstanceTask(timestamp int64, callback *entity.Callback) (taskId string, err error) {
	if len(strconv.FormatInt(timestamp, 10)) != 10 {
		return "", errTaskTimeInvalid
	}
	duration := timestamp - time.Now().Unix()
	if duration < 0 {
		return "", errTaskTimeInvalid
	}
	taskId = snowflake.GetSnowFlakeId()
	task := entity.XcronTask{
		TaskId:     taskId,
		TaskHash:   atomic.AddInt64(&global.XcronState.StoreInstanceTaskCount, 1),
		Type:       entity.TaskTypeInstance,
		State:      entity.TaskStateQueue,
		Format:     strconv.FormatInt(timestamp, 10),
		Callback:   callback,
		CreateTime: time.Now().Unix(),
	}
	data, _ := json.Marshal(&task)
	if err := engine.EnQueue(data); err != nil {
		log.Printf("[error][application.AddInstanceTask] task EnQueue err:%s", err.Error())
		return "", errTaskInstanceEnQueue
	}
	return taskId, nil
}

//add crontab task
func AddCrontabTask(crontab string, callback *entity.Callback) (taskId string, err error) {
	if _, err := cron.ParseStandard(crontab); err != nil {
		return "", errTaskCronInvalid
	}
	taskId = snowflake.GetSnowFlakeId()
	task := entity.XcronTask{
		TaskId:     taskId,
		TaskHash:   atomic.AddInt64(&global.XcronState.StoreCrontabTaskCount, 1),
		Type:       entity.TaskTypeCrontab,
		State:      entity.TaskStateQueue,
		Format:     crontab,
		Callback:   callback,
		CreateTime: time.Now().Unix(),
	}
	data, _ := json.Marshal(&task)
	if err := engine.EnQueue(data); err != nil {
		log.Printf("[error][application.AddCrontabTask] task EnQueue err:%s", err.Error())
		return "", errTaskCrontabEnQueue
	}
	return taskId, nil
}
