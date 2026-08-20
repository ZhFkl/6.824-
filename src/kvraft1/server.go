package kvraft

import (
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type valueEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu  sync.Mutex
	me  int
	rsm *rsm.RSM

	data map[string]valueEntry
	// Your definitions here.
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch args := req.(type) {
	case rpc.GetArgs:
		entry, exists := kv.data[args.Key]
		if !exists {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}

		return rpc.GetReply{
			Value:   entry.Value,
			Version: entry.Version,
			Err:     rpc.OK,
		}
	case rpc.PutArgs:
		entry, exist := kv.data[args.Key]
		if !exist {
			if args.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.data[args.Key] = valueEntry{
				Value:   args.Value,
				Version: 1,
			}
			return rpc.PutReply{Err: rpc.OK}
		}

		if args.Version != entry.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}

		entry.Value = args.Value
		entry.Version++
		kv.data[args.Key] = entry
		return rpc.PutReply{Err: rpc.OK}

	default:
		panic("unknown operation")
	}

	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, result := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = result.(rpc.PutReply)

}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me:   me,
		data: make(map[string]valueEntry),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
