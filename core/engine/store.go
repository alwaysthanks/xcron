package engine

//store package as store use raft engine
func GetData(key string) ([]byte, error) {
	return engine.getData(key)
}

func SetData(key string, val []byte) error {
	return engine.setData(key, val)
}

func AtomCounter(key string) (int64, error) {
	return engine.atomIncr(key)
}

func GetActivePeersCount() int64 {
	return engine.activePeers.Len()
}

func GetActivePeerIndex(addr string) int64 {
	return engine.activePeers.Index(addr)
}
