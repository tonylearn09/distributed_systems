package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Cid    int64 "client unique id"
	SeqNum int   "each request with a monotonically increasing sequence number"
}

type PutAppendReply struct {
	//WrongLeader bool
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Time int64
}

type GetReply struct {
	//WrongLeader bool
	Err   Err
	Value string
}

type MigrateArgs struct {
	Shard     int
	ConfigNum int
}

type MigrateReply struct {
	Err       Err
	ConfigNum int
	Shard     int
	DB        map[string]string
	Cid2Seq   map[int64]int
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
