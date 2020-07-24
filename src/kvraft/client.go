package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

const RetryInterval = time.Duration(50 * time.Millisecond)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader int
	// This Clerk's id (this is a unique identifier)
	clientId int64
	// tag each request with a monotonically increasing sequence number
	serialNumber int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.serialNumber = 0
	ck.lastLeader = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	index := ck.lastLeader
	for {
		args := GetArgs{key}
		reply := GetReply{}
		ok := ck.servers[index].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return reply.Value
		}
		index = (index + 1) % len(ck.servers)
		// TODO: check if need to have this retry interval
		//time.Sleep(RetryInterval)

	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	index := ck.lastLeader
	args := PutAppendArgs{key, value, op, ck.clientId, ck.serialNumber}
	ck.serialNumber++
	for {
		reply := PutAppendReply{}
		ok := ck.servers[index].Call("KVServer.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader {
			ck.lastLeader = index
			return
		}
		index = (index + 1) % len(ck.servers)
		// TODO: check if need to have this retry interval
		//time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
