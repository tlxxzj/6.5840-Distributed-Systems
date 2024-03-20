package kvsrv

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd

	id  int64        // client id
	seq atomic.Int64 // sequence number, increase by 1 for each request
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.

	ck.id = nrand() // generate a random client id
	ck.seq.Store(0) // initialize sequence number to 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.call(OpGet, key, "")
}

func (ck *Clerk) Put(key string, value string) {
	ck.call(OpPut, key, value)
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.call(OpAppend, key, value)
}

func (ck *Clerk) call(op OperationType, key string, value string) string {
	args := &Args{
		Op:       op,
		Key:      key,
		Value:    value,
		ClientId: ck.id,
		Seq:      ck.seq.Add(1), // increase sequence number by 1
	}

	reply := &Reply{}

	ok := false
	for !ok {
		ok = ck.server.Call("KVServer.RemoteCall", args, reply)
	}

	return reply.Value
}
