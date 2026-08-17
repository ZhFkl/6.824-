package lock

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here'
	lockname string
	clientID string
	// zhe li bu ying gai hai you yige suo ma ?
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockname: lockname,
		clientID: kvtest.RandValue(8),
	}

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here

	// zhe li de Acquire shi huo de dui yiing de suo ma ?
	// ye jiu shi ci shi de Lock de han shu ,
	/*
		shei you lock ? yikn
	*/

	/*

		zhe li pan duan ci shi suo shi bu shi kong xian ,
		ru guo shi kong xian jiu huo de suo ran hou fan hui
		ru guo bu shi kong xian de hua ci shi jiu deng dai, ke bu ke yi sleep?
	*/
	for {
		// shou xian tong guo get han shu lai de dao ci shi de kV
		// ran hou pan duan zhuang tai ru guo shi ErrnoKey de hua zheng mign ci shi zhe ge suo hai bu cun zai
		// ru guo huo de le ci shi de value zhi hou
		/*
			ci shi de dao ve
			value version Err lei xing
			value shi ci shi de chi you zhe de ming zi ,version hui pan duan ci shi wo men de ban ben dui bu dui
			ru guo ci shi shi kong de hua na me ci shi wo men jie xia lai yao diao yong put han shu deng dai ci shi version
			shi bu shi bei wo men xiu gai le ,zhe ge yao kan ci shi de version lei xing

		*/
		owner, version, err := lk.ck.Get(lk.lockname)
		switch {
		case err == rpc.ErrNoKey:
			err = lk.ck.Put(lk.lockname, lk.clientID, 0)

		case err == rpc.OK && owner == "":
			err = lk.ck.Put(lk.lockname, lk.clientID, version)

		case err == rpc.OK:
			time.Sleep(10 * time.Millisecond)
			continue
		default:
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if err == rpc.OK {
			return
		}

		if err == rpc.ErrMaybe {
			owner, _, getErr := lk.ck.Get(lk.lockname)
			if getErr == rpc.OK && owner == lk.clientID {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}

}

func (lk *Lock) Release() {
	// Your code here
	/*
		pan duan ci shi suo zai bu zai zi ji zhe li, ru guo zai de hua ci shi jiu release
		ru guo bu zai de hua zhi jie jiu fan hui
	*/

	for {
		owner, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			return
		}
		if err != rpc.OK {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if owner != lk.clientID {
			return
		}

		err = lk.ck.Put(lk.lockname, "", version)

		if err == rpc.OK {
			return
		}

		if err == rpc.ErrMaybe {
			currentOwner, _, getErr := lk.ck.Get(lk.lockname)
			if getErr == rpc.ErrNoKey {
				return
			}

			if getErr == rpc.OK && currentOwner != lk.clientID {
				return
			}
		}

		time.Sleep(10 * time.Millisecond)
	}
}
