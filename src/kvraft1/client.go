package kvraft

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	mu      sync.Mutex
	clnt    *tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt,
		servers: servers,
		leader:  0}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	// zhe li wo xu yao gou zao ci shi de args he reply
	// ran hou ci shi call servers , dan shi zhe li de string shi shen me ? wo zen me cong ci shi
	// wo de client zhogn cha ? wo de client zen me cha?
	args := rpc.GetArgs{
		Key: key,
	}
	reply := rpc.GetReply{}

	for {
		ck.mu.Lock()
		server := ck.leader

		ok := ck.clnt.Call(ck.servers[server], "KVServer.Get", &args, &reply)

		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:

				ck.leader = server
				ck.mu.Unlock()
				return reply.Value, reply.Version, reply.Err
			}

		}

		ck.leader = (server + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	// zhe li ye shi gou zao dui ying de

	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	reply := rpc.PutReply{}
	firstRpc := true
	for {
		ck.mu.Lock()
		server := ck.leader

		ok := ck.clnt.Call(ck.servers[server], "KVServer.Put", &args, &reply)
		if ok {
			switch reply.Err {
			case rpc.OK, rpc.ErrNoKey:
				ck.leader = server
				ck.mu.Unlock()
				return reply.Err

			case rpc.ErrVersion:
				ck.leader = server
				ck.mu.Unlock()
				if firstRpc {
					return rpc.ErrVersion
				}
				return rpc.ErrMaybe
			}
		}
		firstRpc = false
		ck.leader = (server + 1) % len(ck.servers)
		ck.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}
