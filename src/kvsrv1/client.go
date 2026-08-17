package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	// zai zhe li chuang jian ci shi de clerk , zhi hou ke yi tong guo chuang jian de
	// clerk dui xiang lai diao yong ci shi de put he get han shu

	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.

	// zhe li shi get han shu
	// shou xian chuang jian cis hide args de can shu
	// zhe ge chan shu
	/*

		type GetArgs struct {
		Key string
		}

		type GetReply struct {
			Value   string
			Version Tversion
			Err     Err
		}

		can shu lei xing shi zheyang de , chuang jian yige kong de reply
		dang args he reply chuang jian wan cheng zhi hou jiu ke yi diao yong ci shi de call han shu
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)

		ran hou du qu ci shi reply zhong de zhi
		zhe li yao you yige tiao jian pan duan
		if ok != nil{
		}else {

		}

	*/
	//fmt.Printf("clint 开始 get \n")
	args := rpc.GetArgs{
		Key: key,
	}
	for {
		var reply rpc.GetReply
		ok := ck.clnt.Call(
			ck.server,
			"KVServer.Get",
			&args,
			&reply,
		)

		if ok {
			switch reply.Err {
			case rpc.OK:
				return reply.Value, reply.Version, rpc.OK
			case rpc.ErrNoKey:
				return "", 0, rpc.ErrNoKey

			}
		}
	}

	return "", 0, rpc.ErrNoKey
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	/*
		type PutArgs struct {
		Key     string
		Value   string
		Version Tversion
		}

		type PutReply struct {
			Err Err
		}
		zhe li de laing ge can shu shi zheyang de , cishi de key he value yi jing gei ni le
		zhi jie chu shi hua ci shi de putargs ran hou chuang jian yhige kong de reply , chuang jian wan cheng shi hou
		diao yong huan shu
		 ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)

		 zhe ge deng dai chu li , ran hou tong guo ok zhe ge zhi lai pan duan ci shi
		 cao zuo you mei you cheng gong
	*/
	//fmt.Printf("clint 开始 put \n")
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}
	retried := false
	for {
		var reply rpc.PutReply

		ok := ck.clnt.Call(
			ck.server,
			"KVServer.Put",
			&args,
			&reply,
		)
		if ok {
			if reply.Err == rpc.ErrVersion && retried {
				return rpc.ErrMaybe
			}
			return reply.Err
		}
		retried = true
		time.Sleep(100 * time.Millisecond)
	}

}
