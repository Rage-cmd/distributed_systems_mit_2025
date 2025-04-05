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
	// You may add code here
	clientID  string
	lockState string
	// lockStateMap map[string]string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.clientID = kvtest.RandValue(8)
	lk.lockState = l

	// lk.ck.Put(lk.lockState, "u", 0)
	_, _, err := lk.ck.Get(lk.lockState)
	if err == rpc.ErrNoKey {
		// fmt.Printf("Creating a lock, putting in the server\n")
		// for {
		lk.ck.Put(lk.lockState, "u", 0)
		// fmt.Printf("Putting in the server: %v\n", err)
		// if err != rpc.ErrNoKey {
		// break
		// }
	}
	// err := lk.ck.Put(lk.lockState, "u", 0)
	// if err != rpc.OK {
	// 	panic(err)
	// }
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// fmt.Printf("acquiring the lock\n")

	// fmt.Printf("Received values: %s, %d, %v\n", value, version, err)

	for {
		value, version, err := lk.ck.Get(lk.lockState)
		if err != rpc.OK {
			panic(err)
		}

		if value != "u" {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		err = lk.ck.Put(lk.lockState, lk.clientID, version)

		if err == rpc.ErrMaybe {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if err != rpc.OK {
			panic(err)
		}
		break
		// value, version, err = lk.ck.Get(lk.lockState)
		// if err != rpc.OK {
		// 	panic(err)
		// }
	}

}

func (lk *Lock) Release() {
	// Your code here
	value, version, err := lk.ck.Get(lk.lockState)
	if err != rpc.OK {
		panic(err)
	}
	if value != lk.clientID {
		panic("cannot release as the lock was not acquired")
	}
	err = lk.ck.Put(lk.lockState, "u", version)
	if err != rpc.OK && err != rpc.ErrMaybe {
		panic(err)
	}
}
