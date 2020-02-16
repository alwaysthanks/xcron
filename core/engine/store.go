package engine

import "log"

//store package as store use raft engine

//GetData for get key from raftdb
func GetData(key string) ([]byte, error) {
	return engine.getData(key)
}

//SetData for get key from raftdb.
//it try 3 times for unexpected error
func SetData(key string, val []byte) error {
	var err error
	//retry 3 times
	for i := 1; i <= 3; i++ {
		if err = engine.setData(key, val); err == nil {
			break
		} else {
			log.Printf("[error][engine.SetData][%d] engine setData error. key:%s,err:%s", i, key, err.Error())
		}
	}
	return err
}

//AtomCounter for distribute lock increment
func AtomCounter(key string) (int64, error) {
	return engine.atomIncr(key)
}

//GetActivePeersCount for get active machine count
func GetActivePeersCount() int64 {
	return engine.activePeers.Len()
}

//GetActivePeerIndex for get active machine index by it's addr
//use "ordered list algorithm" to get addr order index
func GetActivePeerIndex(addr string) int64 {
	return engine.activePeers.Index(addr)
}
