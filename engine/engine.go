package engine

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/alwaysthanks/xcron/dispatch"
	"github.com/alwaysthanks/xcron/global"
	"github.com/alwaysthanks/xcron/lib/http"
	"github.com/alwaysthanks/xcron/lib/json"
	"github.com/alwaysthanks/xcron/lib/list"
	"github.com/alwaysthanks/xcron/lib/lock"
	"github.com/alwaysthanks/xcron/lib/snowflake"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"github.com/robfig/cron"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	XcronRaftDbFile          = "raft.db"
	XcronRaftTimeout         = 5 * time.Second
	XcronRetainSnapshotCount = 2 // duplicate copy
)

var (
	ShutdownError      = errors.New("engine was shutdown")
	AlreadyOpenedError = errors.New("engine was already opened")
	KeyNotFoundError   = errors.New("key not exist in storeDb")
)

// xcron Engine
type Engine struct {
	logger *log.Logger
	//http transport
	httpTransport *HttpTransport
	//is open to work
	opened bool
	//key-value storeDb for the system.
	storeMutex sync.Mutex
	storeDb    map[string][]byte
	//consensus mechanism
	raftDir  string
	raftBind string
	raft     *raft.Raft
	logStore *raftboltdb.BoltStore
	//distribute atom lock
	atomLock *lock.Lock
	//dispatcher
	dispatcher *dispatch.Dispatcher
	//generation counter
	activePeers *list.SingleOrderList
}

// New returns a new Engine.
//bindAddr: ip:port
func NewEngine(raftDir string, bindAddr string) (*Engine, error) {
	//init
	logger := log.New(os.Stderr, "[xcron] ", log.LstdFlags)
	engine := &Engine{
		logger:      logger,
		storeDb:     make(map[string][]byte),
		raftDir:     raftDir,
		raftBind:    bindAddr,
		atomLock:    lock.NewLockWithTTl(300), //300s ttl
		activePeers: list.NewSingleList(40),   //40s ttl
	}
	//dispatcher
	prevHook := engine.setTaskState(dispatch.TaskStatePrev)
	afterHook := engine.setTaskState(dispatch.TaskStateAfter)
	dispatcher, err := dispatch.NewDispatcher(logger, prevHook, afterHook)
	if err != nil {
		return nil, err
	}
	engine.dispatcher = dispatcher
	return engine, nil
}

// Open opens the engine.
func (engine *Engine) Open() error {
	if engine.opened {
		return AlreadyOpenedError
	}
	engine.opened = true
	//set Raft config
	config := raft.DefaultConfig()
	config.Logger = engine.logger
	//allow the node to entry single-mode
	if len(global.XcronConf.GetPeers()) < 2 {
		engine.logger.Printf("[info][Engine.Open] enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}
	//transport
	transport := NewHTTPTransport(engine, engine.raftBind, engine.logger)
	transport.Start()
	engine.httpTransport = transport
	//create peer storage.
	peerStore := raft.NewJSONPeers(engine.raftDir, engine.httpTransport)
	//create the snapshot storeDb. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(engine.raftDir, XcronRetainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot storeDb: %s", err)
	}
	//create the log storeDb and stable storeDb.
	engine.logStore, err = raftboltdb.NewBoltStore(filepath.Join(engine.raftDir, XcronRaftDbFile))
	if err != nil {
		return fmt.Errorf("new bolt storeDb: %s", err)
	}
	//init the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(engine), engine.logStore, engine.logStore, snapshots, peerStore, engine.httpTransport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	engine.raft = ra
	return nil
}

//close the engine after stepping down as raft node/leader.
func (engine *Engine) Close() error {
	shutdownFuture := engine.raft.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		engine.logger.Printf("[error][engine.Close] raft shutdown err:%s", err.Error())
		return err
	}
	if err := engine.logStore.Close(); err != nil {
		engine.logger.Printf("[error][engine.Close] raft boltdb shutdown err:%s", err.Error())
		return err
	}
	engine.logger.Printf("[info][engine.Close] successfully shutdown")
	return nil
}

var (
	errTaskTimeInvalid = errors.New("instance task time invalid")
	errTaskCronInvalid = errors.New("crontab task invalid")
)

//add instance task
func (engine *Engine) AddInstanceTask(timestamp int64, callback *dispatch.Callback) (taskId string, err error) {
	if len(strconv.FormatInt(timestamp, 10)) != 10 {
		return "", errTaskTimeInvalid
	}
	duration := timestamp - time.Now().Unix()
	if duration < 0 {
		return "", errTaskTimeInvalid
	}
	taskId = snowflake.GetSnowFlakeId()
	task := dispatch.XcronTask{
		TaskId:     taskId,
		TaskHash:   atomic.AddInt64(&global.XcronState.StoreInstanceTaskCount, 1),
		Type:       dispatch.TaskTypeInstance,
		State:      dispatch.TaskStateStore,
		Format:     strconv.FormatInt(timestamp, 10),
		Callback:   callback,
		CreateTime: time.Now().Unix(),
	}
	if err := engine.setTask(&task); err != nil {
		log.Printf("[error][Dispatcher.AddInstanceTask] engine setTask err:%s", err.Error())
		return "", fmt.Errorf("engine setTask error")
	}
	return taskId, nil
}

//add crontab task
func (engine *Engine) AddCrontabTask(crontab string, callback *dispatch.Callback) (taskId string, err error) {
	if _, err := cron.ParseStandard(crontab); err != nil {
		return "", errTaskCronInvalid
	}
	taskId = snowflake.GetSnowFlakeId()
	task := dispatch.XcronTask{
		TaskId:     taskId,
		TaskHash:   atomic.AddInt64(&global.XcronState.StoreCrontabTaskCount, 1),
		Type:       dispatch.TaskTypeCrontab,
		State:      dispatch.TaskStateStore,
		Format:     crontab,
		Callback:   callback,
		CreateTime: time.Now().Unix(),
	}
	if err := engine.setTask(&task); err != nil {
		log.Printf("[error][Dispatcher.AddCrontabTask] engine setTask err:%s", err.Error())
		return "", fmt.Errorf("engine setTask error")
	}
	return taskId, nil
}

//get task
func (engine *Engine) GetTask(taskId string) (*dispatch.XcronTask, error) {
	taskInfo, err := engine.getData(taskId)
	if err != nil {
		return nil, err
	}
	var task = dispatch.XcronTask{}
	if err := json.Unmarshal(taskInfo, &task); err != nil {
		return nil, err
	}
	return &task, nil
}

//join a node reachable under raftAddr
func (engine *Engine) Join(raftAddr string) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	if engine.raft.State() == raft.Leader {
		return engine.join(raftAddr)
	}
	//request leader
	engine.logger.Printf("[info][Engine.Join] Join operation to leader:%s", engine.raft.Leader())
	reqUrl := fmt.Sprintf("http://%s/xcron/peer", engine.raft.Leader())
	reqBody := TransportRequest{
		Cmd:  "join",
		Data: raftAddr,
	}
	if _, err := engine.leaderRequest(reqUrl, reqBody); err != nil {
		engine.logger.Printf("[error][Engine.Join] engine.leaderRequest err:%s", err.Error())
		return err
	}
	return nil
}

func (engine *Engine) join(addr string) error {
	engine.logger.Printf("[info][Engine.join] received join request for remote node as:%s", addr)
	f := engine.raft.AddPeer(addr)
	if f.Error() != nil {
		engine.logger.Printf("[error][Engine.join] node at %s joined failed:%s", addr, f.Error().Error())
		return f.Error()
	}
	engine.logger.Printf("[info][Engine.join] node at %s joined success", addr)
	//add global conf
	global.XcronConf.AddPeer(addr)
	return nil
}

//dispatch task for which machine work and type to run
func (engine *Engine) dispatchTask(task *dispatch.XcronTask) error {
	//current serial number
	localAddr := global.XcronState.GetLocalAddr()
	machineActiveCount := engine.activePeers.Len()
	//serial mod number
	serialNumber := engine.activePeers.Index(localAddr) - 1
	planBNumber := serialNumber - 1
	if planBNumber < 0 {
		planBNumber = machineActiveCount - 1
	}
	if serialNumber < 0 || machineActiveCount == 0 {
		engine.logger.Printf("[error][engine.dispatchTask] get serial number error. machineActiveCount:%d,serialNumber:%d", machineActiveCount, serialNumber)
		return fmt.Errorf("serial count error")
	}
	mod := task.TaskHash % machineActiveCount
	engine.logger.Printf("[info][engine.dispatchTask] task begin dispatch. machine info:host:%s,machineActiveCount:%d,serialNumber:%d, mod:%d", localAddr, machineActiveCount, serialNumber, mod)
	//if only one machine, not planB
	if serialNumber != 0 && mod == planBNumber {
		//double check
		engine.logger.Printf("[info][engine.dispatchTask] check for planB task. taskId:%s,host:%d", task.TaskId, localAddr)
		time.Sleep(time.Second * 10)
		newTask, err := engine.GetTask(task.TaskId)
		if err != nil {
			engine.logger.Printf("[error][engine.dispatchTask] check for planB task. GetTask error. taskId:%d,err:%s", task.TaskId, err.Error())
			return err
		}
		if newTask.State != dispatch.TaskStateStore {
			engine.logger.Printf("[info][engine.dispatchTask] check for planB task. task run normally.taskId:%s", err.Error())
			return nil
		}
		engine.logger.Printf("[info][engine.dispatchTask] task run in planB.taskId:%s, planB host:%s", task.TaskId, localAddr)
	} else if mod != serialNumber {
		engine.logger.Printf("[info][engine.dispatchTask] not this machine for work. host:%s, machineActiveCount:%d,serialNumber:%d, mod:%d", localAddr, machineActiveCount, serialNumber, mod)
		return nil
	}
	//invoke
	var timerId string
	switch task.Type {
	case dispatch.TaskTypeInstance:
		timestamp, _ := strconv.ParseInt(task.Format, 10, 64)
		timerId, _ = engine.dispatcher.SetInstanceTask(task.TaskId, timestamp)
	case dispatch.TaskTypeCrontab:
		timerId, _ = engine.dispatcher.SetCrontabTask(task.TaskId, task.Format)
	default:
		return fmt.Errorf("not support task type")
	}
	task.TimerId = timerId
	task.State = dispatch.TaskStateSet
	task.Host = localAddr
	return engine.setData(task.TaskId, task.Encode())
}

//setTaskState for dispatcher hook
func (engine *Engine) setTaskState(state int16) dispatch.Hook {
	return func(taskId string) (task *dispatch.XcronTask, err error) {
		task, err = engine.GetTask(taskId)
		if err != nil {
			engine.logger.Printf("[error][engine.setTaskState] GetTask error. taskId:%s,err:%s", taskId, err.Error())
			return nil, err
		}
		switch state {
		case dispatch.TaskStatePrev:
			//before.atomic.Incr
			key := fmt.Sprintf("task_id:%s;run_times:%d;state:%d", taskId, task.RunTimes, state)
			counter, err := engine.distributeAtomIncr(key)
			if err != nil {
				engine.logger.Printf("[error][engine.setTaskState] distributeAtomIncr error.key:%s,err:%s", key, err.Error())
				return nil, err
			}
			if counter != 1 {
				engine.logger.Printf("[info][engine.setTaskState] distributeAtomIncr concurrent.key:%s,counter:%d", key, counter)
				return nil, fmt.Errorf("concurrency run task")
			}
			task.State = state
		case dispatch.TaskStateAfter:
			task.State = state
			//after.RunCount++
			task.RunTimes++
		default:
			return nil, fmt.Errorf("not support state:%d", state)
		}
		if err := engine.setTask(task); err != nil {
			engine.logger.Printf("[info][engine.setTaskState] setTask error. taskId:%s,err:%s", taskId, err.Error())
			return nil, err
		}
		return task, nil
	}
}

//atom incr for distribute key.
func (engine *Engine) distributeAtomIncr(key string) (int64, error) {
	if err := engine.checkState(); err != nil {
		return 0, err
	}
	if engine.raft.State() != raft.Leader {
		engine.logger.Printf("[info][Engine.distributeAtomIncr] distributeAtomIncr operation to leader:%s", engine.raft.Leader())
		reqUrl := fmt.Sprintf("http://%s/xcron/peer", engine.raft.Leader())
		reqBody := TransportRequest{
			Cmd:  "incr",
			Data: key,
		}
		ret, err := engine.leaderRequest(reqUrl, reqBody)
		if err != nil {
			engine.logger.Printf("[error][Engine.distributeAtomIncr] engine.leaderRequest err:%s", err.Error())
			return 0, err
		}
		count, _ := strconv.ParseInt(ret, 10, 64)
		return count, nil
	}
	//todo fix
	//crontab task set for every 1s
	// distribute lock key expire 300s,
	// in 2s, repeat task cannot, is error
	return engine.atomLock.Incr(key), nil
}

type operation struct {
	Op    string
	Key   string
	Value []byte
}

//getData returns the value for the given key.
func (engine *Engine) getData(key string) ([]byte, error) {
	if err := engine.checkState(); err != nil {
		return []byte{}, err
	}
	engine.storeMutex.Lock()
	defer engine.storeMutex.Unlock()
	if val, ok := engine.storeDb[key]; ok {
		return val, nil
	}
	return []byte{}, KeyNotFoundError
}

//set task for raft distributeCmd then to dispatch.
func (engine *Engine) setTask(task *dispatch.XcronTask) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	taskInfo, _ := json.Marshal(task)
	c := &operation{
		Op:    "task",
		Key:   task.TaskId,
		Value: taskInfo,
	}
	sc, _ := serializeOperation(c)
	if err := engine.distributeCmd("store", sc); err != nil {
		engine.logger.Printf("[error][Engine.setTask] distributeCmd err:%s", err.Error())
		return err
	}
	return nil
}

//setData sets the value for the given key.
func (engine *Engine) setData(key string, value []byte) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	c := &operation{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	sc, _ := serializeOperation(c)
	if err := engine.distributeCmd("store", sc); err != nil {
		engine.logger.Printf("[error][Engine.setData] distributeCmd err:%s", err.Error())
		return err
	}
	return nil
}

//deleteData deletes the given key.
func (engine *Engine) deleteData(key string) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	c := &operation{
		Op:  "delete",
		Key: key,
	}
	sc, _ := serializeOperation(c)
	if err := engine.distributeCmd("store", sc); err != nil {
		engine.logger.Printf("[error][Engine.deleteData] distributeCmd err:%s", err.Error())
		return err
	}
	return nil
}

//check if shutdown
func (engine *Engine) checkState() error {
	if engine.raft.State() == raft.Shutdown {
		return ShutdownError
	}
	return nil
}

//distributeCmd for distribute operation to leader machine log.
func (engine *Engine) distributeCmd(reqCmd string, reqData []byte) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	if _, err := deserializeOperation([]byte(reqData)); err != nil {
		engine.logger.Printf("[error][Engine.distributeCmd] reqData deserialize err:%s", err.Error())
		return err
	}
	if engine.raft.State() != raft.Leader {
		engine.logger.Printf("[info][Engine.distributeCmd] distributeCmd operation to leader:%s", engine.raft.Leader())
		reqUrl := fmt.Sprintf("http://%s/xcron/peer", engine.raft.Leader())
		reqBody := TransportRequest{
			Cmd:  reqCmd,
			Data: string(reqData),
		}
		if _, err := engine.leaderRequest(reqUrl, reqBody); err != nil {
			engine.logger.Printf("[error][Engine.distributeCmd] engine.leaderRequest err:%s", err.Error())
			return err
		}
		return nil
	}
	f := engine.raft.Apply(reqData, XcronRaftTimeout)
	if err, ok := f.(error); ok {
		return err
	}
	return f.Error()
}

var httpClient = http.NewHttpClient(3, time.Second)

//request leader info
func (engine *Engine) leaderRequest(reqUrl string, reqBody interface{}) (string, error) {
	reqBytes, _ := json.Marshal(reqBody)
	resp, err := httpClient.Post(reqUrl, bytes.NewReader(reqBytes))
	if err != nil {
		engine.logger.Printf("[error][Engine.leaderRequest] http request Post err:%s", err.Error())
		return "", err
	}
	var transResp TransportResponse
	if err := json.NewDecoder(bytes.NewReader(resp)).Decode(&transResp); err != nil {
		engine.logger.Printf("[error][Engine.leaderRequest] http response decode err:%s", err.Error())
		return "", err
	}
	if transResp.Code != 0 {
		engine.logger.Printf("[error][Engine.leaderRequest] http resp code not zero. code:%d,err:%s", transResp.Code, transResp.Error)
		return "", fmt.Errorf(transResp.Error)
	}
	return transResp.Data, nil
}
