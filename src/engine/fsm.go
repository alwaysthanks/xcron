package engine

import (
	"bytes"
	"dispatch"
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/raft"
	"io"
	"lib/json"
)

type fsm Engine

// Apply applies a Raft log entry to the key-value storeDb.
func (f *fsm) Apply(l *raft.Log) interface{} {
	dsc, err := deserializeOperation(l.Data)
	if err != nil {
		f.logger.Fatalf("[fatal][fsm.Apply] error in deserializeOperation: %s", err.Error())
	}
	switch dsc.Op {
	case "set":
		return f.applySet(dsc.Key, dsc.Value)
	case "delete":
		return f.applyDelete(dsc.Key)
	case "task":
		if ret := f.applyTask(dsc.Value); ret != nil {
			return ret
		}
		return f.applySet(dsc.Key, dsc.Value)
	default:
		f.logger.Fatalf("[fatal][fsm.Apply] unrecognized operation op: %s", dsc.Op)
		return nil
	}
}

// Snapshot returns a snapshot of the key-value storeDb.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.storeMutex.Lock()
	defer f.storeMutex.Unlock()
	// Clone the map.
	o := make(map[string][]byte)
	for k, v := range f.storeDb {
		o[k] = v
	}
	return &fsmSnapshot{store: o}, nil
}

// Restore stores the key-value storeDb to a previous state.
func (f *fsm) Restore(rc io.ReadCloser) error {
	o := make(map[string][]byte)
	decoder := gob.NewDecoder(rc)
	err := decoder.Decode(&o)
	if err != nil {
		return err
	}
	// setData the state from the snapshot.
	f.storeDb = o
	return nil
}

func (f *fsm) applyTask(taskInfo []byte) interface{} {
	task := dispatch.XcronTask{}
	if err := json.Unmarshal(taskInfo, &task); err != nil {
		return err
	}
	return (*Engine)(f).dispatchTask(&task)
}

func (f *fsm) applySet(key string, value []byte) interface{} {
	f.storeMutex.Lock()
	defer f.storeMutex.Unlock()
	f.storeDb[key] = value
	return nil
}

func (f *fsm) applyDelete(key string) interface{} {
	f.storeMutex.Lock()
	defer f.storeMutex.Unlock()
	delete(f.storeDb, key)
	return nil
}

type fsmSnapshot struct {
	store map[string][]byte
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

func serializeOperation(c *operation) ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(c)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func deserializeOperation(sc []byte) (*operation, error) {
	if len(sc) < 1 {
		return nil, fmt.Errorf("zero length serialization passed")
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
