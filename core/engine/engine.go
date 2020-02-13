package engine

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/alwaysthanks/xcron/core/global"
	"github.com/alwaysthanks/xcron/core/lib/http"
	"github.com/alwaysthanks/xcron/core/lib/json"
	"github.com/alwaysthanks/xcron/core/lib/list"
	"github.com/alwaysthanks/xcron/core/lib/lock"
	"github.com/grandecola/bigqueue"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	XcronRaftDbFile          = "raft.db"
	XcronRaftTimeout         = 3 * time.Second
	XcronRetainSnapshotCount = 2 // duplicate copy
)

var (
	ShutdownError    = errors.New("engine was shutdown")
	KeyNotFoundError = errors.New("key not exist in database")
)

var engine *RaftEngine

func Init(dir string, bindAddr string) error {
	engine = NewEngine(dir, bindAddr)
	return engine.Start()
}

func Close() error {
	return engine.Close()
}

// xcron RaftEngine
type RaftEngine struct {
	raftDir  string
	raftBind string
	logger   *log.Logger
	//consensus mechanism
	raft     *raft.Raft
	logStore *raftboltdb.BoltStore
	//http transport
	httpTransport *HttpTransport
	//key-value database for the system.
	dbmutex  sync.Mutex
	database map[string][]byte
	//distribute atom lock
	atomLock *lock.Lock
	//generation counter
	activePeers *list.SingleOrderList
	//queue
	bigQueue *bigqueue.MmapQueue
	//mq queue for the system.
	mqmutex sync.Mutex
	mqueue  map[string][]byte
}

// New returns a new RaftEngine.
//bindAddr: ip:port
func NewEngine(dir string, bindAddr string) *RaftEngine {
	//init
	logger := log.New(os.Stderr, "[xcron-engine] ", log.LstdFlags)
	engine := &RaftEngine{
		logger:      logger,
		database:    make(map[string][]byte),
		mqueue:      make(map[string][]byte),
		raftDir:     dir,
		raftBind:    bindAddr,
		atomLock:    lock.NewLockWithTTl(300), //300s ttl
		activePeers: list.NewSingleList(40),   //40s ttl
	}
	return engine
}

//Start opens the engine.
func (engine *RaftEngine) Start() error {
	//set Raft config
	config := raft.DefaultConfig()
	config.Logger = engine.logger
	//allow the node to entry single-mode
	if len(global.XcronConf.GetPeers()) < 2 {
		engine.logger.Printf("[info][RaftEngine.Start] enabling single-node mode")
		config.EnableSingleNode = true
		config.DisableBootstrapAfterElect = false
	}
	//transport
	transport := NewHTTPTransport(engine, engine.raftBind, engine.logger)
	transport.Start()
	engine.httpTransport = transport
	//create peer storage.
	peerStore := raft.NewJSONPeers(engine.raftDir, engine.httpTransport)
	//create the snapshot database. This allows the Raft to truncate the log.
	snapshots, err := raft.NewFileSnapshotStore(engine.raftDir, XcronRetainSnapshotCount, os.Stderr)
	if err != nil {
		return fmt.Errorf("file snapshot database: %s", err)
	}
	//create the log database and stable database.
	engine.logStore, err = raftboltdb.NewBoltStore(filepath.Join(engine.raftDir, XcronRaftDbFile))
	if err != nil {
		return fmt.Errorf("new bolt database: %s", err)
	}
	//init the Raft systems.
	ra, err := raft.NewRaft(config, (*fsm)(engine), engine.logStore, engine.logStore, snapshots, peerStore, engine.httpTransport)
	if err != nil {
		return fmt.Errorf("new raft: %s", err)
	}
	engine.raft = ra
	//init big queue
	queue, err := bigqueue.NewMmapQueue(engine.raftDir, bigqueue.SetArenaSize(4*1024*1024), bigqueue.SetMaxInMemArenas(32))
	if err != nil {
		return fmt.Errorf("new big queue error. err:%s", err.Error())
	}
	engine.bigQueue = queue
	return nil
}

//Close the engine after stepping down as raft node/leader.
func (engine *RaftEngine) Close() error {
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

//JoinPeer join a node reachable under raftAddr
func (engine *RaftEngine) JoinPeer(raftAddr string) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	if engine.raft.State() == raft.Leader {
		return engine.join(raftAddr)
	}
	//request leader
	engine.logger.Printf("[info][RaftEngine.JoinPeer] JoinPeer operation to leader:%s", engine.raft.Leader())
	reqUrl := fmt.Sprintf("http://%s/xcron/peer", engine.raft.Leader())
	reqBody := TransportRequest{
		Cmd:  "join",
		Data: raftAddr,
	}
	if _, err := engine.leaderRequest(reqUrl, reqBody); err != nil {
		engine.logger.Printf("[error][RaftEngine.JoinPeer] engine.leaderRequest err:%s", err.Error())
		return err
	}
	return nil
}

//join local as leader join machine
func (engine *RaftEngine) join(addr string) error {
	engine.logger.Printf("[info][RaftEngine.join] received join request for remote node as:%s", addr)
	f := engine.raft.AddPeer(addr)
	if f.Error() != nil {
		engine.logger.Printf("[error][RaftEngine.join] node at %s joined failed:%s", addr, f.Error().Error())
		return f.Error()
	}
	engine.logger.Printf("[info][RaftEngine.join] node at %s joined success", addr)
	//add global conf
	global.XcronConf.AddPeer(addr)
	return nil
}

//atomIncr for distribute key.
func (engine *RaftEngine) atomIncr(key string) (int64, error) {
	if err := engine.checkState(); err != nil {
		return 0, err
	}
	if engine.raft.State() != raft.Leader {
		engine.logger.Printf("[info][RaftEngine.atomIncr] atomIncr operation to leader:%s", engine.raft.Leader())
		reqUrl := fmt.Sprintf("http://%s/xcron/peer", engine.raft.Leader())
		reqBody := TransportRequest{
			Cmd:  "incr",
			Data: key,
		}
		ret, err := engine.leaderRequest(reqUrl, reqBody)
		if err != nil {
			engine.logger.Printf("[error][RaftEngine.atomIncr] engine.leaderRequest err:%s", err.Error())
			return 0, err
		}
		count, _ := strconv.ParseInt(ret, 10, 64)
		return count, nil
	} else {
		//rand 1-10ms sleep
		number := rand.New(rand.NewSource(time.Now().UnixNano())).Int63n(10) + 1
		time.Sleep(time.Millisecond * time.Duration(number))
	}
	return engine.atomLock.Incr(key), nil
}

type operation struct {
	Op    string
	Key   string
	Value []byte
}

//getData returns the value for the given key.
func (engine *RaftEngine) getData(key string) ([]byte, error) {
	if err := engine.checkState(); err != nil {
		return []byte{}, err
	}
	engine.dbmutex.Lock()
	defer engine.dbmutex.Unlock()
	if val, ok := engine.database[key]; ok {
		return val, nil
	}
	return []byte{}, KeyNotFoundError
}

//setData sets the value for the given key.
func (engine *RaftEngine) setData(key string, value []byte) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	c := &operation{
		Op:    "set",
		Key:   key,
		Value: value,
	}
	sc, _ := encodeOperation(c)
	if err := engine.executeCmd("store", sc); err != nil {
		engine.logger.Printf("[error][RaftEngine.setData] executeCmd err:%s", err.Error())
		return err
	}
	return nil
}

//deleteData deletes the given key.
func (engine *RaftEngine) deleteData(key string) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	c := &operation{
		Op:  "delete",
		Key: key,
	}
	sc, _ := encodeOperation(c)
	if err := engine.executeCmd("store", sc); err != nil {
		engine.logger.Printf("[error][RaftEngine.deleteData] executeCmd err:%s", err.Error())
		return err
	}
	return nil
}

//queueGet get queue info for the given key.
func (engine *RaftEngine) queueGet(key string) ([]byte, error) {
	if err := engine.checkState(); err != nil {
		return []byte{}, err
	}
	engine.mqmutex.Lock()
	defer engine.mqmutex.Unlock()
	if val, ok := engine.mqueue[key]; ok {
		return val, nil
	}
	return []byte{}, KeyNotFoundError
}

//queueSet sets queue info for the given key.
func (engine *RaftEngine) queueSet(key string, value []byte) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	c := &operation{
		Op:    "queue",
		Key:   key,
		Value: value,
	}
	sc, _ := encodeOperation(c)
	if err := engine.executeCmd("store", sc); err != nil {
		engine.logger.Printf("[error][RaftEngine.queueSet] executeCmd err:%s", err.Error())
		return err
	}
	return nil
}

//check if shutdown
func (engine *RaftEngine) checkState() error {
	if engine.raft.State() == raft.Shutdown {
		return ShutdownError
	}
	return nil
}

//executeCmd for distribute operation to leader machine log.
func (engine *RaftEngine) executeCmd(reqCmd string, reqData []byte) error {
	if err := engine.checkState(); err != nil {
		return err
	}
	if _, err := decodeOperation([]byte(reqData)); err != nil {
		engine.logger.Printf("[error][RaftEngine.executeCmd] reqData deserialize err:%s", err.Error())
		return err
	}
	if engine.raft.State() != raft.Leader {
		engine.logger.Printf("[info][RaftEngine.executeCmd] executeCmd operation to leader:%s", engine.raft.Leader())
		reqUrl := fmt.Sprintf("http://%s/xcron/peer", engine.raft.Leader())
		reqBody := TransportRequest{
			Cmd:  reqCmd,
			Data: string(reqData),
		}
		if _, err := engine.leaderRequest(reqUrl, reqBody); err != nil {
			engine.logger.Printf("[error][RaftEngine.executeCmd] engine.leaderRequest err:%s", err.Error())
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
func (engine *RaftEngine) leaderRequest(reqUrl string, reqBody interface{}) (string, error) {
	reqBytes, _ := json.Marshal(reqBody)
	resp, err := httpClient.Post(reqUrl, bytes.NewReader(reqBytes))
	if err != nil {
		engine.logger.Printf("[error][RaftEngine.leaderRequest] http request Post err:%s", err.Error())
		return "", err
	}
	var transResp TransportResponse
	if err := json.NewDecoder(bytes.NewReader(resp)).Decode(&transResp); err != nil {
		engine.logger.Printf("[error][RaftEngine.leaderRequest] http response decode err:%s", err.Error())
		return "", err
	}
	if transResp.Code != 0 {
		engine.logger.Printf("[error][RaftEngine.leaderRequest] http resp code not zero. code:%d,err:%s", transResp.Code, transResp.Error)
		return "", fmt.Errorf(transResp.Error)
	}
	return transResp.Data, nil
}

type fsm RaftEngine

// Apply applies a Raft log entry to the key-value database.
func (f *fsm) Apply(l *raft.Log) interface{} {
	dsc, err := decodeOperation(l.Data)
	if err != nil {
		f.logger.Fatalf("[fatal][fsm.Apply] error in decodeOperation: %s", err.Error())
	}
	switch dsc.Op {
	case "set":
		return f.applySet(dsc.Key, dsc.Value)
	case "delete":
		return f.applyDelete(dsc.Key)
	case "queue":
		return f.applyQueue(dsc.Key, dsc.Value)
	default:
		f.logger.Fatalf("[fatal][fsm.Apply] unrecognized operation op: %s", dsc.Op)
		return nil
	}
}

// Snapshot returns a snapshot of the key-value database.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	store := make(map[string]map[string][]byte)
	// Clone store map.
	f.dbmutex.Lock()
	o := make(map[string][]byte)
	for k, v := range f.database {
		o[k] = v
	}
	store["s"] = o
	f.dbmutex.Unlock()
	// Clone queue map.
	f.mqmutex.Lock()
	q := make(map[string][]byte)
	for k, v := range f.mqueue {
		q[k] = v
	}
	store["q"] = q
	f.mqmutex.Unlock()
	return &fsmSnapshot{store: store}, nil
}

// Restore stores the key-value database to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string]map[string][]byte)
	decoder := gob.NewDecoder(rc)
	err := decoder.Decode(&o)
	if err != nil {
		return err
	}
	// setData the state from the snapshot.
	f.database = o["s"]
	f.mqueue = o["q"]
	return nil
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.dbmutex.Lock()
	defer f.dbmutex.Unlock()
	f.database[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.dbmutex.Lock()
	defer f.dbmutex.Unlock()
	delete(f.database, key)
	return nil
}

func (f *fsm) applyQueue(key string, value []byte) interface{} {
	f.mqmutex.Lock()
	defer f.mqmutex.Unlock()
	//queue
	if _, ok := f.mqueue[key]; !ok {
		var item queueItem
		json.Unmarshal(value, &item)
		if item.State == StateEnqueueData {
			f.bigQueue.Enqueue(item.Data)
		}
	}
	//store
	f.mqueue[key] = value
	return nil
}

type fsmSnapshot struct {
	store map[string]map[string][]byte
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	closure := func() error {
		// Encode data.
		buf := bytes.NewBuffer([]byte{})
		encoder := gob.NewEncoder(buf)
		err := encoder.Encode(f.store)
		if err != nil {
			return err
		}
		var n int
		// Write data to sink.
		if n, err = sink.Write(buf.Bytes()); err != nil {
			return err
		}
		if n != buf.Len() {
			return fmt.Errorf("incomplete write for snapshot")
		}
		// Close the sink.
		if err := sink.Close(); err != nil {
			return err
		}
		return nil
	}
	if err := closure(); err != nil {
		sink.Cancel()
		return err
	}
	return nil
}

func (f *fsmSnapshot) Release() {
	//TODO snapshot release function
}

func encodeOperation(c *operation) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(c)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeOperation(sc []byte) (*operation, error) {
	if len(sc) < 1 {
		return nil, fmt.Errorf("zero length decodeOperation passed")
	}
	buf := bytes.NewBuffer(sc)
	decoder := gob.NewDecoder(buf)
	command := &operation{}
	err := decoder.Decode(command)
	if err != nil {
		return nil, err
	}
	return command, nil
}
