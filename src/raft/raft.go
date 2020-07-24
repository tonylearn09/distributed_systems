package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	SnapShot     []byte
}

// Server Has 3 states (Follower, Candidate, Leader)
type ServerState int

const (
	Follower  ServerState = iota // value = 0
	Candidate                    // value = 1
	Leader                       // value = 2
)

func (s ServerState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		panic("unreachable")
	}
}

// Different intervals
const (
	heartBeatInterval    = 100 * time.Millisecond
	electionTimeoutLower = 300
)

// Null to indicate votedFor is None at the beginning
const (
	nullVotedFor int = -1
	nullTerm     int = -1
)

// Log format
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent Raft state on all servers
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile Raft state on all servers
	commitIndex int
	lastApplied int
	state       ServerState

	// Volatile Raft state on leaders
	nextIndex  []int
	matchIndex []int

	// channel where this raft server is going to report committed log
	// entries. It's passed in by the client during construction.
	applyCh chan ApplyMsg
	// Kill Signal
	killCh chan bool
	// Channels to handle RPC
	voteCh      chan bool
	appendLogCh chan bool

	// Log compaction
	lastIncludedIndex int
	lastIncludedTerm  int
}

func (rf *Raft) String() string {
	return fmt.Sprintf("[index(%d), state(%v), term(%d)]", rf.me, rf.state, rf.currentTerm)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// When you change rf.currentTerm, rf.votedFor, or rf.logs, you should call persist()
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	rf.persister.SaveRaftState(rf.encodeRaftState())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), snapshot)
}

func (rf *Raft) encodeRaftState() []byte {
	// TODO: check whether need lock
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
	var lastIncludedIndex, lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		//DPrintf("%v fails to recover from persistant", rf)
		log.Fatalf("readPersist ERROR for server %v", rf.me)
		return
	}

	rf.mu.Lock()
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm

	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex
	rf.mu.Unlock()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
// Check Figure 2 in paper
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
// Check Figure 2 in paper
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// All servers rule Figure 2
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}

	// Figure 2 receiver implementation
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor != nullVotedFor && rf.votedFor != args.CandidateId {
		reply.VoteGranted = false
	} else if args.LastLogTerm < rf.getLastLogTerm() ||
		(args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex < rf.getLastLogIndex()) {
		// The candidate's log is not as up to date
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.persist()
		// break out the go select statement in make
		send(rf.voteCh)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC
// Check Figure 2 in paper
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	// Optimization (https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt)
	// How to roll back quickly part
	// first index it stores for that conflict term"
	ConflictIndex int
	// the term of the conflicting entry
	ConflictTerm int
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		// Only need the reply.Term information to make old leader become follower
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Figure 2 all servers rule
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
	}
	// signal receiving heartbeat
	send(rf.appendLogCh)

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = nullTerm
	reply.ConflictIndex = 0

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm
	// Use the optimization described in https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt
	// (how to roll back quickly
	prevLogIndexTerm := nullTerm
	if args.PrevLogIndex >= rf.lastIncludedIndex && args.PrevLogIndex < rf.logLen() {
		prevLogIndexTerm = rf.getLog(args.PrevLogIndex).Term
	}
	if prevLogIndexTerm != args.PrevLogTerm {
		if prevLogIndexTerm == nullTerm {
			// follower does not have prevLogIndex in its log
			reply.ConflictIndex = rf.logLen()
		} else {
			reply.ConflictTerm = prevLogIndexTerm
			for i := rf.lastIncludedIndex; i < rf.logLen(); i++ {
				// Find the first index whose entry has term equal to conflictTerm
				if rf.getLog(i).Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}
		}
		return
	}

	// Success part to append log
	index := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i++ {
		index++
		if index < rf.logLen() {
			if rf.getLog(index).Term == args.Entries[i].Term {
				continue
			} else {
				// If an existing entry conflicts with a new one (same index
				// but different terms), delete the existing entry and all that
				// follow it (Figure 2)
				rf.logs = rf.logs[:index-rf.lastIncludedIndex]
			}
		}
		// Append any new entries not already in the log
		rf.logs = append(rf.logs, args.Entries[i:]...)
		rf.persist()
		break
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.getLastLogIndex())
		rf.updateLastApplied()
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Install Snapshot RPC
// Slightly different from Figure 13
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// raw bytes of the snapshot chunks
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// Reply immediately if term < currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		// all server rule (Figure 2)
		// If RPC request or response contains term T > currentTerm
		// set currentTerm = T, convert to follower (§5.1)
		rf.becomeFollower(args.Term)
	}
	send(rf.appendLogCh)

	if args.LastIncludedIndex <= rf.getLastLogIndex() {
		// discard snapshot with a smaller index
		return
	}

	applyMsg := ApplyMsg{CommandValid: false, SnapShot: args.Data}
	if args.LastIncludedIndex < rf.logLen()-1 {
		// still need to keep the logs after args.LastIncludedIndex
		// Note that we include args.LastIncludedIndex as our dummy variable for the 0th index
		rf.logs = append(make([]LogEntry, 0), rf.logs[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
	} else {
		// dummy variable for the 0th index
		rf.logs = []LogEntry{{nil, args.LastIncludedTerm}}
	}
	rf.lastIncludedIndex, rf.lastIncludedTerm = args.LastIncludedIndex, args.LastIncludedTerm
	rf.persistWithSnapshot(args.Data)
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)

	if rf.lastApplied > rf.lastIncludedIndex {
		// snapshot doesn't cover all lastApplied
		// IT is older than kvserver's db, and no need to send to rf.applyCh
		return
	}
	rf.applyCh <- applyMsg
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := rf.currentTerm
	isLeader := (rf.state == Leader)

	if isLeader {
		// Figure 2
		// If command received from client: append entry to local log,
		// respond after entry applied to state machine
		index = rf.getLastLogIndex() + 1
		newLogEntry := LogEntry{command, rf.currentTerm}
		rf.logs = append(rf.logs, newLogEntry)
		rf.persist()
		rf.startAppendLog()
	}

	return index, term, isLeader
}

//
// Snapshot section
//
func (rf *Raft) sendSnapshot(server int) {
	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludedIndex,
		rf.lastIncludedTerm,
		rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, reply)
	if ok {
		// The rpc call succeed
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.state != Leader || rf.currentTerm != args.Term {
			return
		} else if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		} else {
			rf.updateNextMatchIndex(server, rf.lastIncludedIndex)
			return
		}
	} else {
		// The rpc call fail
		DPrintf("Communication error: sendInstallSnapshot() RPC "+
			"failed for server %d", server)
	}
}

// Log compaction
func (rf *Raft) DoSnapshot(idx int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if idx <= rf.lastIncludedIndex {
		// Already in snapshot
		return
	}
	// update logs to have rf.lastIncludedIndex and the following only
	rf.logs = append(make([]LogEntry, 0), rf.logs[idx-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = idx
	rf.lastIncludedTerm = rf.getLog(idx).Term
	rf.persistWithSnapshot(snapshot)
}

//
// Leader section
//
func (rf *Raft) becomeLeader() {
	// Lock in the caller

	// Only can jump from candidate to leader in the startElection()
	/*
		if rf.state != Candidate {
			return
		}
	*/
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
}

func (rf *Raft) startAppendLog() {
	// Use as either heartbeat or append log
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// Issue AppendEntries RPC in parallel to all peers
		// RPC call should be in a go routine
		go func(idx int) {
			for {
				rf.mu.Lock()
				if rf.state != Leader {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[idx] <= rf.lastIncludedIndex {
					// issue new RPC InstallSnapshot to followers that
					// are too far behind
					// sendSnapshot, and it will update the logs simultaneously
					rf.sendSnapshot(idx)
					// Note: need to release the Lock in sendSnapshot
					return
				}

				args := AppendEntriesArgs{
					rf.currentTerm,
					rf.me,
					rf.getPrevLogIndex(idx),
					rf.getPrevLogTerm(idx),
					// rf.nextIndex[idx] - rf.lastIncludedIndex is the actual nextIndex in the logs
					// If nextIndex > last log index, it behaves like an heartbeat with empty elemnt
					// otherwise, we send AppendEntries RPC with log entries starting at the actual nextIndex
					append(make([]LogEntry, 0), rf.logs[rf.nextIndex[idx]-rf.lastIncludedIndex:]...),
					rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(idx, &args, reply)
				if ok {
					rf.mu.Lock()
					if rf.state != Leader || rf.currentTerm != args.Term {
						// The sender itself has change its state already
						// This may happens because it takes a long time to get the reply
						// In this case, nothing to do
						rf.mu.Unlock()
						return
					} else if reply.Term > rf.currentTerm {
						rf.becomeFollower(reply.Term)
						rf.mu.Unlock()
						return
					} else if reply.Success {
						rf.updateNextMatchIndex(idx, args.PrevLogIndex+len(args.Entries))
						rf.mu.Unlock()
						return
					} else {
						// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
						// handle mismatch prevLogIndex and prevLogTerm
						conflictIndex := reply.ConflictIndex
						// Check https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt
						if reply.ConflictTerm != nullTerm {
							// Case 1 and Case 2 in https://pdos.csail.mit.edu/6.824/notes/l-raft2.txt
							logSize := rf.logLen()
							for i := rf.lastIncludedIndex; i < logSize; i++ {
								if rf.getLog(i).Term != reply.ConflictTerm {
									continue
								}
								for i < logSize && rf.getLog(i).Term == reply.ConflictTerm {
									i++
								}
								// set nextIndex to be the one
								// beyond the index of the last entry in that term in its log
								conflictIndex = i
								break
							}
						}
						// In case 3, conflictIndex is the len of follower's log
						rf.nextIndex[idx] = Min(rf.logLen(), conflictIndex)
						rf.mu.Unlock()
					}

				} else {
					// TODO
					// If followers crash or run slowly,
					// or if network packets are lost, the leader retries Append-
					// Entries RPCs indefinitely
					DPrintf("Communication error: sendAppendEntries() RPC "+
						"failed for server %d", idx)
					return
				}

			}
		}(i)
	}
}

// Helper to update matchIndex and nextIndex slices
// It is called by leader only
func (rf *Raft) updateNextMatchIndex(server, matchIdx int) {
	// Caller holds the lock
	rf.matchIndex[server] = matchIdx
	rf.nextIndex[server] = matchIdx + 1
	rf.updateCommitIndex()
}

// Update the commitIndex
// It is called by leader only
func (rf *Raft) updateCommitIndex() {
	// Caller holds the lock

	// Figure 2
	// If there exists an N such that N > commitIndex,
	// and a majority of matchIndex[i] ≥ N,
	// and log[N].term == currentTerm:
	// set commitIndex = N
	rf.matchIndex[rf.me] = rf.logLen() - 1
	copyMatchIndex := make([]int, len(rf.matchIndex))
	copy(copyMatchIndex, rf.matchIndex)
	sort.Sort(sort.Reverse(sort.IntSlice(copyMatchIndex)))
	N := copyMatchIndex[len(copyMatchIndex)/2]
	if N > rf.commitIndex && rf.getLog(N).Term == rf.currentTerm {
		rf.commitIndex = N
		rf.updateLastApplied()
	}
}

//
// Candidate section
//
func (rf *Raft) becomeCandidate() {
	// Lock from caller, no need to do it here

	// Increase current term, vote for itself
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()

	go rf.startElection()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastLogIndex(),
		rf.getLastLogTerm(),
	}
	rf.mu.Unlock()

	// Originally, vote for itself
	var voteCount int32 = 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// Issue RequestVote RPC in parallel to all peers
		// RPC call should be in a go routine
		go func(index int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(index, &args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					// the candidate itself is lagged behind
					rf.becomeFollower(reply.Term)
					// Force the canadidate to break out the select case
					send(rf.voteCh)
					return
				} else if rf.state != Candidate || rf.currentTerm != args.Term {
					// The sender itself has change its state already
					// This may happens because it takes a long time to get the reply
					// In this case, nothing to do
					return
				} else if reply.VoteGranted {
					atomic.AddInt32(&voteCount, 1)
				}

				// If we get majority votes
				if atomic.LoadInt32(&voteCount) > int32(len(rf.peers)/2) {
					rf.becomeLeader()
					// sending heartbeat
					rf.startAppendLog()
					// break out the periodic check select statement
					send(rf.voteCh)
				}
			} else {
				DPrintf("Communication error: sendRequestVote() RPC "+
					"failed for server %d", index)
			}
		}(i)
	}
}

//
// Follower section
//
func (rf *Raft) becomeFollower(term int) {
	// Lock from the caller, no need to lock inside
	rf.state = Follower
	rf.votedFor = nullVotedFor
	rf.currentTerm = term
	rf.persist()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	send(rf.killCh)
}

//
// All Server's rule (no matter what state)
//
func (rf *Raft) updateLastApplied() {
	// Caller hold the lock
	// If commitIndex > lastApplied: increment lastApplied,
	// apply log[lastApplied] to state machine

	// We use Max(..., rf.lastIncludedIndex) to guard against
	// rf.lastApplied < rf.lastIncludedIndex
	// Note: we know that our lastApplied should be at least
	// lastIncludedIndex because we have take the snapshot
	rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
	rf.commitIndex = Max(rf.commitIndex, rf.lastIncludedIndex)
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		curLog := rf.getLog(rf.lastApplied)
		applyMsg := ApplyMsg{true, curLog.Command, rf.lastApplied, nil}
		rf.applyCh <- applyMsg
	}
}

//
// Helper function Section
//

// Helper for sending on channel
func send(ch chan bool) {
	select {
	case <-ch:
		// drain the channel if something in the channel already
		// to avoid blocking
	default:
	}
	ch <- true
}

// Helper for getting log length and logEntry
func (rf *Raft) logLen() int {
	// Note that the index of rf.logs start at 1
	// So if len(rf.logs) == 5, then the last log entry's index is 4
	return len(rf.logs) + rf.lastIncludedIndex
}
func (rf *Raft) getLog(idx int) LogEntry {
	return rf.logs[idx-rf.lastIncludedIndex]
}

// Helper for getting last log index and term (RequestVotes)
func (rf *Raft) getLastLogIndex() int {
	return rf.logLen() - 1
}
func (rf *Raft) getLastLogTerm() int {
	idx := rf.getLastLogIndex()
	if idx < rf.lastIncludedIndex {
		// No knowledge for it as we store as snapshot already
		return nullTerm
	}
	return rf.getLog(idx).Term
}

// Helper for getting previous log index and term (AppendEntries)
func (rf *Raft) getPrevLogIndex(i int) int {
	return rf.nextIndex[i] - 1
}
func (rf *Raft) getPrevLogTerm(i int) int {
	prevLogIndex := rf.getPrevLogIndex(i)
	if prevLogIndex < rf.lastIncludedIndex {
		return nullTerm
	}
	return rf.getLog(prevLogIndex).Term
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = nullVotedFor
	// first index of logs is 1
	// Thus, we use a dummy variable for index 0
	// At index 0, term = 0, Command is nil
	rf.logs = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	// Use buffered channel to avoid go routine leak
	// and non blocking
	rf.killCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
	rf.appendLogCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// create a background goroutine that will kick off leader election/apppend entries periodically
	go rf.periodicCheck()

	return rf
}

// Do the periodCheck to elect new leader, send append entries, etc.
func (rf *Raft) periodicCheck() {
	for {

		// Don't block the receive from killCh
		select {
		case <-rf.killCh:
			return
		default:
		}

		electionTime := time.Duration(electionTimeoutLower+rand.Intn(200)) * time.Millisecond

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		// Better not to hold lock in this part that "wait"
		// So, use a copy of state above when locking
		switch state {
		case Follower, Candidate:
			select {
			// We just want to break out the select in case we receive from voteCh/appendCh
			case <-rf.voteCh:
			case <-rf.appendLogCh:
			case <-time.After(electionTime):
				rf.mu.Lock()
				// upgrade itself to candidate, and start election
				rf.becomeCandidate()
				rf.mu.Unlock()
			}
		case Leader:
			time.Sleep(heartBeatInterval)
			// send heartbeat/append log
			rf.startAppendLog()
		}
	}
}
