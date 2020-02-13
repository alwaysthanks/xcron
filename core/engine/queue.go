package engine

import (
	"crypto/md5"
	"fmt"
	"github.com/alwaysthanks/xcron/core/lib/json"
)

//mq package as queue use raft engine

const (
	StateEnqueueData = 1
	StateDequeueData = 2
)

type queueItem struct {
	State int    `json:"state"`
	Data  []byte `json:"data"`
}

func EnQueue(data []byte) error {
	return setQueue(StateEnqueueData, data)
}

func DeQueue() (data []byte, err error) {
	for {
		data, err = engine.bigQueue.Dequeue()
		if err != nil {
			return nil, err
		}
		key := fmt.Sprintf("%x", md5.Sum(data))
		ret, err := engine.queueGet(key)
		if err != nil {
			return nil, err
		}
		var item queueItem
		json.Unmarshal(ret, &item)
		if item.State == StateEnqueueData {
			break
		}
	}
	//todo ack
	if err := setQueue(StateDequeueData, data); err != nil {
		return nil, err
	}
	return data, nil
}

func setQueue(state int, data []byte) error {
	item := queueItem{
		State: state,
		Data:  data,
	}
	key := fmt.Sprintf("%x", md5.Sum(data))
	val, _ := json.Marshal(&item)
	return engine.queueSet(key, val)
}
