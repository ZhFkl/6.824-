package rsm

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID      int64
	Request any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type submitResult struct {
	OpID  int64
	Value any
	Err   rpc.Err
}

type waiter struct {
	OpID int64
	Ch   chan submitResult
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	waiter       map[int]waiter
	// Your definitions here.
}

func (rsm *RSM) reader() {
	for msg := range rsm.applyCh {
		if !msg.CommandValid {
			continue
		}

		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		value := rsm.sm.DoOp(op.Request)

		rsm.mu.Lock()
		w, exists := rsm.waiter[msg.CommandIndex]
		if !exists {
			rsm.mu.Unlock()
			continue
		}

		delete(rsm.waiter, msg.CommandIndex)

		var result submitResult

		if w.OpID == op.ID {
			result = submitResult{
				OpID:  op.ID,
				Value: value,
				Err:   rpc.OK,
			}
		} else {
			result = submitResult{
				OpID: w.OpID,
				Err:  rpc.ErrWrongLeader,
			}
		}

		rsm.mu.Unlock()

		w.Ch <- result

	}

}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		waiter:       make(map[int]waiter),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func newOpID() int64 {
	max := big.NewInt(int64(1) << 62)

	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		panic(err)
	}
	return n.Int64()

}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	op := Op{
		ID:      newOpID(),
		Request: req,
	}
	ch := make(chan submitResult, 1)
	rsm.mu.Lock()
	index, startTerm, isleader := rsm.rf.Start(op)

	if !isleader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}

	if old, exists := rsm.waiter[index]; exists {
		delete(rsm.waiter, index)
		old.Ch <- submitResult{
			OpID: old.OpID,
			Err:  rpc.ErrWrongLeader,
		}
	}

	rsm.waiter[index] = waiter{
		OpID: op.ID,
		Ch:   ch,
	}

	rsm.mu.Unlock()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case result := <-ch:
			if result.Err != rpc.OK {
				return result.Err, nil
			}

			if result.OpID != op.ID {
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, result.Value
		case <-ticker.C:
			currentTerm, stillLeader := rsm.rf.GetState()
			if !stillLeader || currentTerm != startTerm {
				rsm.mu.Lock()

				current, exists := rsm.waiter[index]
				if exists && current.OpID == op.ID {
					delete(rsm.waiter, index)
					rsm.mu.Unlock()
					return rpc.ErrWrongLeader, nil
				}
				rsm.mu.Unlock()

				result := <-ch
				if result.Err != rpc.OK || result.OpID != op.ID {
					return rpc.ErrWrongLeader, nil
				}

				return rpc.OK, result.Value
			}
		}

	}

	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}
