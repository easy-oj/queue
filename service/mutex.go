package service

import (
	"sync"
	"sync/atomic"
)

type mutexSet struct {
	mutex   *sync.Mutex
	mutexes map[string]*wrappedMutex
}

type wrappedMutex struct {
	count int64
	mutex *sync.Mutex
}

func newMutexSet() *mutexSet {
	return &mutexSet{
		mutex:   &sync.Mutex{},
		mutexes: make(map[string]*wrappedMutex, 4096),
	}
}

func newWrappedMutex() *wrappedMutex {
	return &wrappedMutex{
		mutex: &sync.Mutex{},
	}
}

func (s *mutexSet) Lock(name string) {
	s.mutex.Lock()
	mutex := s.mutexes[name]
	if mutex == nil {
		mutex = newWrappedMutex()
		s.mutexes[name] = mutex
	}
	atomic.AddInt64(&mutex.count, 1)
	s.mutex.Unlock()
	mutex.mutex.Lock()
}

func (s *mutexSet) Unlock(name string) {
	s.mutex.Lock()
	mutex := s.mutexes[name]
	if mutex == nil {
		s.mutex.Unlock()
		return
	}
	count := atomic.AddInt64(&mutex.count, -1)
	if count <= 0 {
		delete(s.mutexes, name)
	}
	s.mutex.Unlock()
	mutex.mutex.Unlock()
}
