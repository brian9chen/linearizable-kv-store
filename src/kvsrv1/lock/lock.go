package lock

import (
	"fmt"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	ck      kvtest.IKVClerk
	key     string
	held    bool
	version rpc.Tversion
	id      string // unique ID so we can tell if OUR Put went through
}

func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:  ck,
		key: l,
		id:  kvtest.RandValue(16),
	}
	ck.Put(l, "available", 0)
	return lk
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err != rpc.OK {
			continue
		}
		// If we already hold it (previous ErrMaybe that actually succeeded)
		if value == lk.id {
			lk.held = true
			lk.version = version
			return
		}
		// Someone else holds it — wait
		if value != "available" {
			continue
		}
		// Try to acquire by writing our unique ID
		err = lk.ck.Put(lk.key, lk.id, version)
		if err == rpc.OK {
			lk.held = true
			lk.version = version + 1
			return
		}
		if err == rpc.ErrMaybe {
			// Check if our Put actually went through
			val, newVer, err2 := lk.ck.Get(lk.key)
			if err2 == rpc.OK && val == lk.id {
				lk.held = true
				lk.version = newVer
				return
			}
		}
		// ErrVersion or ErrMaybe that didn't go through: loop again
	}
}

func (lk *Lock) Release() {
	if !lk.held {
		fmt.Println("release failed: lock not held")
		return
	}
	for {
		err := lk.ck.Put(lk.key, "available", lk.version)
		if err == rpc.OK {
			lk.held = false
			return
		}
		if err == rpc.ErrMaybe {
			// Check if our release went through
			val, _, err2 := lk.ck.Get(lk.key)
			if err2 == rpc.OK && val != lk.id {
				// Value is no longer our ID — release succeeded
				lk.held = false
				return
			}
			// Still our ID — release didn't go through, retry
		}
	}
}
