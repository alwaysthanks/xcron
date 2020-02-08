package list

import (
	"sync"
	"time"
)

//single order expire list
func NewSingleList(ttl int64) *SingleOrderList {
	return &SingleOrderList{
		ttl:  ttl,
		root: &singleItem{}, //placeholder
	}
}

type SingleOrderList struct {
	ttl  int64 //s
	lock sync.Mutex
	root *singleItem
}

type singleItem struct {
	next *singleItem
	val  string
	time int64
}

func (s *SingleOrderList) Index(key string) int64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	prev := s.root
	node := s.root.next
	curTime := time.Now().Unix()
	var i int64
	for node != nil {
		if node.val > key {
			break
		}
		if node.time+s.ttl > curTime {
			i++
		}
		prev = node
		node = node.next
	}
	if prev.val == key {
		return i
	}
	return 0
}

func (s *SingleOrderList) Len() int64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	node := s.root.next
	curTime := time.Now().Unix()
	var i int64
	for node != nil {
		if node.time+s.ttl > curTime {
			i++
		}
		node = node.next
	}
	return i
}

func (s *SingleOrderList) Add(key string) error {
	s.lock.Lock()
	defer s.lock.Unlock()
	prev := s.root
	node := s.root.next
	//empty list
	if node == nil {
		prev.next = &singleItem{val: key, time: time.Now().Unix()}
		return nil
	}
	for node != nil {
		if key < node.val { //header
			prev.next = &singleItem{val: key, time: time.Now().Unix(), next: node}
			break
		} else if key == node.val {
			node.time = time.Now().Unix()
			break
		} else if node.next == nil {
			node.next = &singleItem{val: key, time: time.Now().Unix()}
			break
		}
		prev = node
		node = node.next
	}
	return nil
}
