package routine

import (
	"context"
	"fmt"
	"github.com/alwaysthanks/xcron/lib/uuid"
	"log"
	"runtime/debug"
	"time"
)

//create parallel routine
func NewRoutinePool(parallelRoutineCount int) *Pool {
	if parallelRoutineCount < 1 {
		parallelRoutineCount = 1
	}
	item := &Pool{
		task:     make(chan *Task, parallelRoutineCount),
		sem:      make(chan struct{}, parallelRoutineCount),
		maxCount: int64(parallelRoutineCount),
	}
	go item.schedule()
	return item
}

type Pool struct {
	task chan *Task
	sem  chan struct{}
	//monitor data
	curRunCount    int64
	totalRunCount  int64
	totalFailCount int64
	maxCount       int64
}

func (pool *Pool) Start(ctx context.Context, f TaskFunc) error {
	t := Task{
		ctx:      ctx,
		taskId:   uuid.GetUUId(),
		taskFunc: f,
	}
	return pool.addTask(&t)
}

func (pool *Pool) schedule() {
	for {
		select {
		case t := <-pool.task:
			go func() {
				defer func() {
					<-pool.sem // <- struct{}{}
				}()
				err := t.execute()
				if err != nil {
					pool.totalFailCount++
					log.Printf("[error][Pool.schedule] task exec error. taskId:%s, err:%s", t.taskId, err.Error())
				}
				pool.curRunCount--
				pool.totalRunCount++
			}()
		}
	}
}

func (pool *Pool) addTask(t *Task) error {
	select {
	case pool.sem <- struct{}{}:
		pool.curRunCount++
		pool.task <- t
	case <-t.ctx.Done():
		return fmt.Errorf("routine add task timeout")
	}
	return nil
}

type TaskFunc func(t *Task) error

type Task struct {
	taskId   string
	ctx      context.Context
	taskFunc TaskFunc
}

func (t *Task) GetTaskId() string {
	return t.taskId
}

func (t *Task) execute() (err error) {
	defer func(begin time.Time) {
		if pErr := recover(); pErr != nil {
			log.Printf("[error][Task.Execute] #PANIC# task execute panic. taskId:%s, panic:%v, stack:%s", t.taskId, pErr, string(debug.Stack()))
			err = fmt.Errorf("panic err %v", pErr)
		}
		if err != nil {
			log.Printf("[error][Task.Execute] task execute error. taskId:%s, err:%s", t.taskId, err.Error())
		}
		log.Printf("[info][Task.Execute] task execute over. taskId:%s, timeUsed:%v", t.taskId, time.Since(begin))
	}(time.Now())
	select {
	case <-t.ctx.Done():
		return fmt.Errorf("task execute timeout")
	default:
	}
	return t.taskFunc(t)
}
