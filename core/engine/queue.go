package engine

//mq package as queue use raft engine

//EnQueue make data in queue
func EnQueue(data []byte) error {
	return engine.enQueue(data)
}

type FuncConsume func(data []byte) error

//DeQueue to consume data which enqueue by ack through FuncConsume function
//ack can wipe the raft log re again where the system restart.
func DeQueue(fn FuncConsume) (data []byte, err error) {
	data, err = engine.deQueue()
	if err != nil {
		return nil, err
	}
	if err := fn(data); err != nil {
		return nil, err
	}
	if err := engine.ackQueue(data); err != nil {
		return nil, err
	}
	return data, nil
}
