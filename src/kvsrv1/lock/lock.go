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

	// for {
	_ = lk.ck.Put(lk.lockState, "u", 0)

	// value, _, _ := lk.ck.Get(lk.lockState)

	// if value != "u" {
	// 	time.Sleep(10 * time.Millisecond)
	// 	continue
	// }

	// break
	// }

	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	// fmt.Printf("acquiring the lock\n")

	// fmt.Printf("Received values: %s, %d, %v\n", value, version, err)
	// fmt.Printf("INSIDE ACQUIRE %s-------- \n", lk.clientID)

	for {
		value, version, err := lk.ck.Get(lk.lockState)
		// fmt.Printf("Received values: %s, %d, %v\n", value, version, err)

		if err != rpc.OK {
			panic(err)
		}

		if value == lk.clientID {
			break
		}

		if value != "u" {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		err = lk.ck.Put(lk.lockState, lk.clientID, version)
		// fmt.Printf("Attempting to acquire the lock %s, err => %v. Values received => %s, %d\n", lk.clientID, err, value, version)

		if err == rpc.ErrMaybe {
			value, _, _ = lk.ck.Get(lk.lockState)
			if value == lk.clientID {
				break
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}

		if err != rpc.OK {
			// panic(err)
			continue
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
	// fmt.Printf("INSIDE RELEASE %s-------- \n", lk.clientID)

	value, version, err := lk.ck.Get(lk.lockState)
	// fmt.Printf("Releasing lock: %s, %d, %v\n", value, version, err)

	if err != rpc.OK {
		panic(err)
	}
	// if value != lk.clientID {
	// 	fmt.Printf("Value of the lock is %s, expected %s\n", value, lk.clientID)
	// 	panic("cannot release as the lock was not acquired")
	// }
	for {
		err = lk.ck.Put(lk.lockState, "u", version)

		if err == rpc.ErrMaybe {
			value, version, _ = lk.ck.Get(lk.lockState)

			if value == "u" {
				break
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if err != rpc.OK {
			continue
		}
		// time.Sleep(100 * time.Millisecond)
		break
	}
	// fmt.Printf("Released Lock for verison: %d\n", version)
}
