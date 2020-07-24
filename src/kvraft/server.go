package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"strconv"
	"sync"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	// OpType is the operation type(eg. put/append)
	Operation    string
	Key          string
	Value        string
	ClientID     int64
	SerialNumber int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	persister             *raft.Persister
	database              map[string]string // key Value pair in kvserver
	clienIdToSerialNumber map[int64]int     // most recent processed SerialNumber for a client
	notifyChs             map[int]chan Op   // For every raft's log index, a channel would notify client if done
	killCh                chan bool         // kill kvserver
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// from hint: A simple solution is to enter every Get()
	// (as well as each Put() and Append()) in the Raft log.
	// Thus, if a log is commited by raft, we know that it is agreed by a majority
	// and it does not contain stale data
	// Note that we use nrand() to get very random number. Otherwise,
	// equalOp will be true if args.Key is the same
	originOp := Op{"Get", args.Key, strconv.FormatInt(nrand(), 10), 0, 0}
	reply.WrongLeader = true
	// Note:
	// Below step will run the raft's appendEntries or InstallSnapshot
	// In that function, it will hold raft's lock, and may be block
	// if no one getting ApplyMsg in applyCh
	// However, we have a background go rountine that periodically receive
	// from applyCh, so we are fine
	index, _, isLeader := kv.rf.Start(originOp)

	if !isLeader {
		return
	}

	notifyCh := kv.put(index, true)
	op := kv.beNotified(notifyCh, index)
	if equalOp(originOp, op) {
		// From hint:
		// needs to handle the case in which a leader has called Start() for a Clerk's RPC,
		// but loses its leadership before the request is committed to the log. One way to do this is
		// for the server to detect that it has lost leadership,
		// by noticing that a different request has appeared at the index returned by Start()
		kv.mu.Lock()
		reply.Value = kv.database[args.Key]
		kv.mu.Unlock()
		reply.WrongLeader = false
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// from hint: A simple solution is to enter every Get()
	// (as well as each Put() and Append()) in the Raft log.
	originOp := Op{args.Op, args.Key, args.Value, args.ClientID, args.SerialNumber}
	reply.WrongLeader = true
	index, _, isLeader := kv.rf.Start(originOp)
	if !isLeader {
		return
	}
	notifyCh := kv.put(index, true)
	op := kv.beNotified(notifyCh, index)
	// From hint:
	// needs to handle the case in which a leader has called Start() for a Clerk's RPC,
	// but loses its leadership before the request is committed to the log. One way to do this is
	// for the server to detect that it has lost leadership,
	// by noticing that a different request has appeared at the index returned by Start()
	if equalOp(originOp, op) {
		// No error
		reply.WrongLeader = false
	}
}

//
// notifyChs logic
//
func (kv *KVServer) put(idx int, createIfNotExist bool) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// We never create this notification before
	if _, ok := kv.notifyChs[idx]; !ok {
		if !createIfNotExist {
			return nil
		} else {
			// we don't want the sending to block
			kv.notifyChs[idx] = make(chan Op, 1)
		}
	}
	return kv.notifyChs[idx]
}

func send(ch chan Op, op Op) {
	select {
	case <-ch:
		// drain the channel to prevent blocking first
	default:
	}
	ch <- op
}

func (kv *KVServer) beNotified(notifyCh chan Op, index int) Op {
	timeout := time.Duration(600) * time.Millisecond
	select {
	case notifyArg := <-notifyCh:
		close(notifyCh)
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()
		return notifyArg
	case <-time.After(timeout):
		return Op{}
	}
}

func equalOp(a, b Op) bool {
	return a.Operation == b.Operation && a.Key == b.Key && a.Value == b.Value &&
		a.ClientID == b.ClientID && a.SerialNumber == b.SerialNumber
}

//
// Snapshot logics
//
func (kv *KVServer) needSnapShot() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	threshold := 10
	return kv.maxraftstate > 0 &&
		kv.maxraftstate-kv.persister.RaftStateSize() < kv.maxraftstate/threshold
}

func (kv *KVServer) doSnapShot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// From hint:
	// Need to store the application state (kv.database)
	// and the state to detect duplicated operations in the log (kv.clienIdToSerialNumber)
	kv.mu.Lock()
	e.Encode(kv.database)
	e.Encode(kv.clienIdToSerialNumber)
	kv.mu.Unlock()
	// Make raft do the log compaction
	kv.rf.DoSnapshot(index, w.Bytes())

}

func (kv *KVServer) readSnapShot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		// nothing in it
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var clienIdToSerialNumber map[int64]int
	if d.Decode(&db) != nil || d.Decode(&clienIdToSerialNumber) != nil {
		log.Fatalf("readSnapShot ERROR for server %v", kv.me)
	} else {
		kv.database = db
		kv.clienIdToSerialNumber = clienIdToSerialNumber
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- true
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.database = make(map[string]string)
	kv.notifyChs = make(map[int]chan Op)
	kv.clienIdToSerialNumber = make(map[int64]int)
	// Non blocking
	kv.killCh = make(chan bool, 1)

	kv.readSnapShot(kv.persister.ReadSnapshot())

	go kv.periodicCheck()

	return kv
}

func (kv *KVServer) periodicCheck() {
	for {
		select {
		case <-kv.killCh:
			return
		case applyMsg := <-kv.applyCh:
			if !applyMsg.CommandValid {
				// Getting new snapshot from leader, so update itself
				kv.readSnapShot(applyMsg.SnapShot)
				continue
			}
			op := applyMsg.Command.(Op)
			kv.mu.Lock()
			// Update the latest serial number processed for the kvserver
			// We are sure this applyMst is commited and applied in Raft, so safe to update the map here
			maxSerialNumber, found := kv.clienIdToSerialNumber[op.ClientID]
			if !found || op.SerialNumber > maxSerialNumber {
				// Update database if changes
				switch op.Operation {
				case "Put":
					kv.database[op.Key] = op.Value
				case "Append":
					kv.database[op.Key] += op.Value
				}
				kv.clienIdToSerialNumber[op.ClientID] = op.SerialNumber
			}
			kv.mu.Unlock()

			if kv.needSnapShot() {
				// Should run a go routine for any function that involves raft's lock
				// otherwise, it will deadlock
				// e.g. when we call kv.doSnapShot, which call kv.rf.DoSnapshot, which tries
				// to acquire lock. In the meanwhile, there may be other rpc call to Get/Put/Append
				// that call rf.Start, which do AppendEntries, which also need lock. Now, we have two process both try
				// to acquire the lock. If AppendEntries succeed, and want to send back to applyCh.
				// It is blocked as there is no receiver. Our background periodic check go routine is waiting
				// for the kv.doSnapShot to return, and cannot do the receive
				go kv.doSnapShot(applyMsg.CommandIndex)
			}
			if notifyCh := kv.put(applyMsg.CommandIndex, false); notifyCh != nil {
				// combined send() with buffered channel, this part will not block
				// It is critical to use send() here, as we may receive the same CommandIndex
				// for several times. (check https://thesquareplanet.com/blog/students-guide-to-raft/
				// reappearing indices). In this case, we just keep the last one.
				// As noted in the guide, it may be wrong request, but before returnning to client
				// we will check if the request is exactly the same, and if it is not, we will just
				// ignore it, and client will resend the request if didn't hear from us
				send(notifyCh, op)
			}

		}
	}
}
