package service

import (
	"sync"
	"sync/atomic"

	"github.com/easy-oj/common/proto/queue"
)

type messageQueue struct {
	mutex *sync.Mutex
	size  int64
	head  *messageQueueNode
	tail  *messageQueueNode
}

type messageQueueNode struct {
	message *queue.Message
	next    *messageQueueNode
}

func newMessageQueue() *messageQueue {
	node := &messageQueueNode{}
	return &messageQueue{
		mutex: &sync.Mutex{},
		head:  node,
		tail:  node,
	}
}

func (q *messageQueue) Size() int64 {
	return atomic.LoadInt64(&q.size)
}

func (q *messageQueue) Put(message *queue.Message) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.tail.next = &messageQueueNode{
		message: message,
	}
	q.tail = q.tail.next
	atomic.AddInt64(&q.size, 1)
}

func (q *messageQueue) Get() *queue.Message {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if q.head.next == nil {
		return nil
	}
	q.head = q.head.next
	atomic.AddInt64(&q.size, -1)
	return q.head.message
}
