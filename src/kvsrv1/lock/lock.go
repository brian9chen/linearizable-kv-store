package lock

import (
	"fmt"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	key string
	held bool
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l}
	ck.Put(l, "available", 0)
	return lk
}


func (lk *Lock) Acquire() {
	for {
	held, version, err := lk.ck.Get(lk.key)
	if err != rpc.OK {
		fmt.Println(err)
	}
	if held != "held" {
		err = lk.ck.Put(lk.key, "held", version)
		if err == rpc.OK {
			lk.held = true
			lk.version = version + 1
			return
		}
	}
	}
}
func (lk *Lock) Release() {
	if lk.held {
		lk.held = false
		lk.ck.Put(lk.key, "available", lk.version)
	} else {
		fmt.Println("release failed: lock not held")
	}
}
