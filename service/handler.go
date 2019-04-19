package service

import (
	"context"
	"strconv"
	"sync"

	"github.com/easy-oj/common/proto/queue"
)

type queueHandler struct {
	mutexSet *mutexSet
	inQueues *sync.Map
	outQueue *messageQueue
}

func NewQueueHandler() *queueHandler {
	return &queueHandler{
		mutexSet: newMutexSet(),
		inQueues: &sync.Map{},
		outQueue: newMessageQueue(),
	}
}

func (h *queueHandler) PutMessage(ctx context.Context, req *queue.PutMessageReq) (*queue.PutMessageResp, error) {
	channel := strconv.Itoa(int(req.Message.Uid))
	h.mutexSet.Lock(channel)
	defer h.mutexSet.Unlock(channel)
	if inQueue, ok := h.inQueues.Load(channel); ok {
		inQueue.(*messageQueue).Put(req.Message)
	} else {
		h.inQueues.Store(channel, newMessageQueue())
		h.outQueue.Put(req.Message)
	}
	return queue.NewPutMessageResp(), nil
}

func (h *queueHandler) GetMessage(ctx context.Context, req *queue.GetMessageReq) (*queue.GetMessageResp, error) {
	resp := queue.NewGetMessageResp()
	if resp.Message = h.outQueue.Get(); resp.Message == nil {
		return resp, nil
	}
	channel := strconv.Itoa(int(resp.Message.Uid))
	h.mutexSet.Lock(channel)
	defer h.mutexSet.Unlock(channel)
	if inQueue, ok := h.inQueues.Load(channel); !ok {
		return resp, nil
	} else if message := inQueue.(*messageQueue).Get(); message == nil {
		h.inQueues.Delete(channel)
	} else {
		h.outQueue.Put(message)
	}
	return resp, nil
}
