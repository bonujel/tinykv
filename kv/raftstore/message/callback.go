package message

import (
	"sync"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type Callback struct {
	mu   sync.RWMutex
	once sync.Once

	Resp *raft_cmdpb.RaftCmdResponse
	Txn  *badger.Txn // used for GetSnap
	done chan struct{}
}

func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb == nil {
		return
	}
	cb.once.Do(func() {
		if resp != nil {
			cb.mu.Lock()
			cb.Resp = resp
			cb.mu.Unlock()
		}
		cb.done <- struct{}{}
	})
}

func (cb *Callback) SetTxn(txn *badger.Txn) {
	if cb == nil {
		return
	}
	cb.mu.Lock()
	cb.Txn = txn
	cb.mu.Unlock()
}

func (cb *Callback) GetTxn() *badger.Txn {
	if cb == nil {
		return nil
	}
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.Txn
}

func (cb *Callback) getResp() *raft_cmdpb.RaftCmdResponse {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.Resp
}

func (cb *Callback) WaitResp() *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.getResp()
	}
}

func (cb *Callback) WaitRespWithTimeout(timeout time.Duration) *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.getResp()
	case <-time.After(timeout):
		return nil
	}
}

func NewCallback() *Callback {
	done := make(chan struct{}, 1)
	cb := &Callback{done: done}
	return cb
}
