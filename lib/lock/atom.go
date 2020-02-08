package lock

import (
	"github.com/alwaysthanks/xcron/lib/lru"
	"sync"
)

type Lock struct {
	mutex sync.Mutex
	ttl   int
	cache *lru.LRUCache
}

func NewLockWithTTl(ttl int) *Lock {
	return &Lock{
		ttl:   ttl,
		cache: lru.NewLRUCache(100000, ttl),
	}
}

func (lock *Lock) Incr(key string) int64 {
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	var count int64
	if val, err := lock.cache.Get(key); err == nil {
		count = val.(int64)
	}
	count++
	lock.cache.Update(key, count)
	return count
}
