package raftkv

import "log"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// clients assign unique serial numbers to every command
	// so raft know if it is processed
	ClientID     int64
	SerialNumber int
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.

	// No need to have ClientID and SerialNumber here
	// as Get operation won't change the state of kvserver
	// It is not going to update the value for a particular key
	// so re-executing request is fine
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
