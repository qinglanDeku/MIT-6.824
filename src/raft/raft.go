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
	"encoding/binary"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// import "bytes"
// import "6.824/labgob"

// Some global values
// Heartbeat period of leader to send, ms.
var heartbeatCircle = 100
// Election timeout, ms.
var electionTimeoutUpper = 450
var electionTimeoutLower = 300
var defaultLogCapacity = 1000


//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerRole int

const (
	FOLLOWER  ServerRole = iota
	CANDIDATE
	LEADER
)

type LogEntry struct{
	term		int			// Term of this log entry
	// todo: other data type will be added to stored main info of the log entry
}

func (le *LogEntry) toBytes() []byte{
	var ret	[]byte
	err := binary.Write(bytes.NewBuffer(ret[:]), binary.LittleEndian, le)
	if err != nil{
		log.Println(err.Error())
		// If failed to write LogEntry to []byte, then filled a []byte with 0 which has a same size as LogEntry
		err = binary.Write(bytes.NewBuffer(ret[:]), binary.LittleEndian, make([]byte, int(unsafe.Sizeof(LogEntry{}))))
		if err != nil{
			log.Println(err.Error())
			syscall.Exit(-1)
		}
	}
	return ret
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu			sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]
	dead      	int32               // set by Kill()
	role		ServerRole			// Role of this server in the cluster.

	// Update on stable storage before responding to RPC
	currentTerm		int
	votedFor		int				// ID of the raft this raft peer voted for.
	logs			[]byte			// log entries, do not define for now.


	committedIndex	int				// Index of highest entry known to be committed
	lastApplied		int				// Index of highest log entry applied to state machine

	//Reinitialized after election, only held by leaders.
	nextIndex[]		int 			// For each server, index of next log entry sent that
	matchIndex[]	int				// For each server, index of highest log entry known to be replicated on server.


	heartbeatCounter	int			// A counter count for time span of heartbeat call

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = rf.role == LEADER

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term 			int // Candidate's term
	CandidateID		int // Candidate that request vote
	LastLogIndex	int // Index of candidate's last log entry
	LastLogTerm		int // Term of candidate's last log entry
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term			int // currentTerm, for candidate to update itself
	VoteGranted		bool // True means the candidate received vote.
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else if args.Term == rf.currentTerm{
		if rf.votedFor != -1{
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
		}else{
			rf.votedFor = args.CandidateID
			reply.Term = args.Term
			reply.VoteGranted = true
		}
	}else{
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.role = FOLLOWER
	}
	rf.mu.Unlock()
	// TODO: The part of comparing term and index of logs.
	// Your code here (2A, 2B).
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

type AppendEntriesArgs struct{
	// AppendEntries RPCs that carry no log entries is the heartbeat
	Term			int			// Term of current leader
	LeaderID		int			// ID of current leader

	// TODO: add data record about entries. For heartBeat, we only need first two properties.
}

type AppendEntriesReply struct{
	Term			int			// Term of the follower
	Success			bool 		// TODO: used for matching entries
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	// TODO: add entries passing
	rf.mu.Lock()
	if rf.role != FOLLOWER {
		if args.Term >= rf.currentTerm {
			rf.role = FOLLOWER
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.Success = true
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	heartbeatTimeout := heartbeatCircle
	electionTimeout := -1
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		if rf.role == LEADER{
			heartbeatArgs := AppendEntriesArgs{rf.currentTerm, rf.me}
			for i := 0; i < len(rf.peers); i++{
				heartbeatReply := AppendEntriesReply{}
				if i == rf.me{
					continue
				}
				rf.sendAppendEntries(i, &heartbeatArgs, &heartbeatReply)
				// TODO: handle failed reply
			}
		}else if rf.role == FOLLOWER{

		}


	}
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
	// basic properties
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	rf.role = FOLLOWER

	// Update on stable storage before responding to RPC
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.committedIndex = 0
	// Set default size of logs to 1000
	rf.logs = make([]byte, defaultLogCapacity * int(unsafe.Sizeof(LogEntry{})))

	// Reinitialized after election, only held by leaders.
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Other properties
	rf.heartbeatCounter = heartbeatCircle
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
