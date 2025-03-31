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

type ValueVersionPair struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// map of string to struct of type {string, int64}
	kvmap map[string]ValueVersionPair
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kvmap = make(map[string]ValueVersionPair)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, exists := kv.kvmap[args.Key]; exists {
		reply.Value = value.value
		reply.Version = value.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	if args.Version == 0 {
		// if the key already exists, then the verison error is returned
		if _, exists := kv.kvmap[args.Key]; exists {
			reply.Err = rpc.ErrVersion
		} else {
			// installing the key if the version is 0 and it is not in the map
			kv.kvmap[args.Key] = ValueVersionPair{
				value:   args.Value,
				version: args.Version + 1,
			}
			reply.Err = rpc.OK
		}
		kv.mu.Unlock()
		return
	}

	if _, exists := kv.kvmap[args.Key]; exists {
		if kv.kvmap[args.Key].version == args.Version {
			kv.kvmap[args.Key] = ValueVersionPair{
				value:   args.Value,
				version: args.Version + 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		reply.Err = rpc.ErrNoKey
	}
	kv.mu.Unlock()
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
