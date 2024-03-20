package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OperationResult struct {
	Seq   int64
	Value string
}

type KVServer struct {
	mu sync.Mutex

	data         map[string]string          // key-value store
	lastClientOp map[int64]*OperationResult // last operation result for each client
}

// detect duplicate operation
// return true if the operation is duplicate
func (kv *KVServer) handleDuplicateOp(args *Args, reply *Reply) bool {
	lastOp, ok := kv.lastClientOp[args.ClientId]
	if ok && lastOp.Seq == args.Seq {
		// same operation, return the same result
		reply.Value = lastOp.Value
		return true
	}
	return false
}

// Get value for the key, if the key does not exist, return "".
func (kv *KVServer) handleGet(args *Args, reply *Reply) {
	value, ok := kv.data[args.Key]
	if !ok {
		reply.Value = ""
	} else {
		reply.Value = value
	}

	// delete last operation result for the client, release memory
	delete(kv.lastClientOp, args.ClientId)

	// Get operation is idempotent, so we don't need to store the result
}

// Set value for key, if key exists, overwrite the value.
func (kv *KVServer) handlePut(args *Args, reply *Reply) {
	if kv.handleDuplicateOp(args, reply) {
		return
	}

	kv.data[args.Key] = args.Value

	reply.Value = ""

	// Put operation is not idempotent, so we need to store the result
	lastOp, ok := kv.lastClientOp[args.ClientId]
	if ok {
		lastOp.Seq = args.Seq
		lastOp.Value = reply.Value
	} else {
		kv.lastClientOp[args.ClientId] = &OperationResult{Seq: args.Seq, Value: reply.Value}
	}
}

// Append value to key's value and returns the old value.
// For a non-existent key, it is treated as an empty string.
func (kv *KVServer) handleAppend(args *Args, reply *Reply) {
	if kv.handleDuplicateOp(args, reply) {
		return
	}

	value, ok := kv.data[args.Key]
	reply.Value = value
	if !ok {
		kv.data[args.Key] = args.Value
	} else {
		kv.data[args.Key] = value + args.Value
	}

	// Append operation is not idempotent, so we need to store the result
	lastOp, ok := kv.lastClientOp[args.ClientId]
	if ok {
		lastOp.Seq = args.Seq
		lastOp.Value = reply.Value
	} else {
		kv.lastClientOp[args.ClientId] = &OperationResult{Seq: args.Seq, Value: reply.Value}
	}
}

func (kv *KVServer) RemoteCall(args *Args, reply *Reply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args.Op {
	case OpGet:
		kv.handleGet(args, reply)
	case OpPut:
		kv.handlePut(args, reply)
	case OpAppend:
		kv.handleAppend(args, reply)
	default:
		DPrintf("unknown operation: %v", args.Op)
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.

	kv.data = make(map[string]string)
	kv.lastClientOp = make(map[int64]*OperationResult)

	return kv
}
