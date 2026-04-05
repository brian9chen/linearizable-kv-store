package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type kvEntry struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu    sync.Mutex
	store map[string]kvEntry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		store: make(map[string]kvEntry),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, ok := kv.store[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = entry.value
	reply.Version = entry.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	entry, exists := kv.store[args.Key]
	if !exists {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv.store[args.Key] = kvEntry{value: args.Value, version: 1}
		reply.Err = rpc.OK
		return
	}
	if args.Version != entry.version {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.store[args.Key] = kvEntry{value: args.Value, version: entry.version + 1}
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
