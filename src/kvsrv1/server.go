package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Entry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// zhe li de KVServer xu yao zuo shen me
	//xu yao yi map lai guan li ci shi de kv
	// xu yao yi ge dong xi lai ji lu ci shi de version

	data map[string]Entry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		data: make(map[string]Entry),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	/*
		args
		Key string

		reply
		Value   string
		Version Tversion
		Err     Err
		shou xian tong guo ci shi de key lai zhao value ru guo cun zai de hua na me ci shi jiu fan hui
		dui ying de value , version , zhe ge version zen me fu zhi? wo de map zhong yao fang version ma ?
		hai shi shuo ci shi version shi yige mask ?
		shagn mian chu li wan cheng zhi hou  fan hui
	*/
	//fmt.Printf("服务器开始get\n")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, exists := kv.data[args.Key]
	if !exists {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = entry.value
	reply.Err = rpc.OK
	reply.Version = entry.version
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	// shou xian shi jin xing put cao zuo , dan shi zhe li put fang dao na ne ?
	// suo yi ci shi wo de server xu yao guan li kV de yi ge shu ju jie gou
	// huo zhe shuo shi map ,
	// mei ci lai le zhi hou shou xina chong putargs zhong ti qu chu dui ying de key
	/*
		Key     string
		Value   string
		Version Tversion

		map xian zhao key
		1. key  bu cun zai na me ci shi jiu ba zhe ge kv jie gou cha ru dao mapp zhong
		2. key cun zai dan shi value bu tong , ci shi jiu ba dui ying de value gai cheng xin de value
		3. key cun zai er qie value ye xiang tong , na me ci shi zhe li de version shi zuo shen me de ?

		map wan cheng zao zuo zhi hou
		ci shi bu chong reply zhogn de shu ju
		// Err's returned by server and Clerk
		OK         = "OK"
		ErrNoKey   = "ErrNoKey"
		ErrVersion = "ErrVersion"

		Err Err
		ru guo cheng gong na me ci shi de err = OK
		ru guo zhao bu dao de hua ci shi bu hui chuang jian ma?

		// Update the value for a key if args.Version matches the version of
		// the key on the server. If versions don't match, return ErrVersion.
		// If the key doesn't exist, Put installs the value if the
		// args.Version is 0, and returns ErrNoKey otherwise.
		ru guo ci shi de version == 0 , na me jiu neng zeng jia
		dan shi ru guo version == !0 na zen me ban ? you ban ben xie yi ma ?
	*/
	//fmt.Printf("服务器 kai shi put\n")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	entry, exists := kv.data[args.Key]
	if !exists {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv.data[args.Key] = Entry{
			value:   args.Value,
			version: 1,
		}
		reply.Err = rpc.OK
		return
	}

	if args.Version != entry.version {
		reply.Err = rpc.ErrVersion
		return
	}
	entry.value = args.Value
	entry.version++

	kv.data[args.Key] = entry
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()

	// zhe li MakerKVServer zhi hou ci shi yao zuo shen me ?
	// yao yi zhi zai zhe li xun ran deng dai ci shi de client jin xing lian jie
	// zhe li de xun huan zhi jie yong yi ge for ma ?
	// hai you zai MakeKVServer zhong dui ci shi de server jin xing chu shi hua
	return []any{kv}
}
