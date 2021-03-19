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
	"fmt"
	"labgob"
	"labrpc"
	"log"
	"math"
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
var heartbeatCircle int64 = 100
var electionTimeoutGap = 100
// Election timeout, ms.
var electionTimeoutLower = 175
var defaultLogCapacity = 1000

// debug triggers
var electionDebugEnable = false
var replicationDebugEnable = false

func electionDebug(s string){
	if electionDebugEnable{
		log.Print(s)
	}
}

func replicationDebug(s string){
	if replicationDebugEnable{
		log.Print(s)
	}
}


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
	Term		int			// Term of this log entry
	Index 		int			// Index of the log, begin from 1
	Info		interface{}	// Unknown type of info
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
	applyChan	chan ApplyMsg

	// Update on stable storage before responding to RPC
	CurrentTerm 	int
	VotedFor    	int        // ID of the raft this raft peer voted for.
	Logs        	[]LogEntry // log entries, do not define for now.


	committedIndex	int				// Index of highest entry known to be committed
	lastApplied		int				// Index of highest log entry applied to state machine

	//Reinitialized after election, only held by leaders.
	nextIndex[]		int 			// For each server, index of next log entry sent that
	matchIndex[]	int				// For each server, index of highest log entry known to be replicated on server.


	lastHeartbeatUnixTime		int64			// Record for the system time of last heartbeat (unix time nano)

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) turnToLeader() {
	for i := range rf.nextIndex {
		if i == rf.me {
			continue
		}
		if len(rf.Logs) == 1{
			rf.nextIndex[i] = 1
		}else{
			rf.nextIndex[i] = rf.Logs[len(rf.Logs)-1].Index + 1
			if rf.Logs[len(rf.Logs)-1].Index + 1 != len(rf.Logs){
				log.Printf("Wrong index!%d", len(rf.Logs))
			}
		}
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) GetVoteForLocked() int{
	ret := 0
	rf.mu.Lock()
	ret = rf.VotedFor
	rf.mu.Unlock()
	return ret
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.role == LEADER
	rf.mu.Unlock()

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
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	err := encoder.Encode(rf.CurrentTerm)
	if err != nil{
		log.Printf("Failed to store persist state for peer %d\n", rf.me)
	}
	err = encoder.Encode(rf.VotedFor)
	if err != nil{
		log.Printf("Failed to store persist state for peer %d\n", rf.me)
	}
	err = encoder.Encode(rf.Logs)
	if err != nil{
		log.Printf("Failed to store persist state for peer %d\n", rf.me)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)

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
	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var term int
	var votedFor int
	var logs []LogEntry
	if decoder.Decode(&term) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logs) != nil{
		log.Fatalf("Failed to decode persist data for peer%d\n", rf.me)
	}else{
		rf.mu.Lock()
		rf.CurrentTerm = term
		rf.VotedFor = votedFor
		rf.Logs = logs
		rf.mu.Unlock()
	}
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
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if rf.killed(){
		return
	}
	rf.mu.Lock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
	}else if args.Term == rf.CurrentTerm {
		if rf.VotedFor != -1{
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
		}else{
			if len(rf.Logs) == 1 || args.LastLogTerm > rf.Logs[len(rf.Logs)-1].Term ||
				(args.LastLogTerm == rf.Logs[len(rf.Logs)-1].Term && args.LastLogIndex >= rf.Logs[len(rf.Logs)-1].Index){
				rf.role = FOLLOWER
				rf.VotedFor = args.CandidateID
				rf.CurrentTerm = args.Term
				rf.persist()
				rf.lastHeartbeatUnixTime = time.Now().UnixNano()
				reply.Term = args.Term
				reply.VoteGranted = true
				electionDebug(fmt.Sprintf("Peer %d voted for Peer %d::%v, %v, %v, 1. Logs: %v", rf.me, args.CandidateID,
					len(rf.Logs) == 1, args.LastLogTerm > rf.Logs[len(rf.Logs)-1].Term,
					args.LastLogTerm == rf.Logs[len(rf.Logs)-1].Term && args.LastLogIndex >= rf.Logs[len(rf.Logs)-1].Index,
					rf.Logs))
			}else{
				reply.Term = args.Term
				reply.VoteGranted = false
			}

		}
	}else{
		if len(rf.Logs) == 1 || args.LastLogTerm > rf.Logs[len(rf.Logs)-1].Term ||
			(args.LastLogTerm == rf.Logs[len(rf.Logs)-1].Term && args.LastLogIndex >= rf.Logs[len(rf.Logs)-1].Index) {
			rf.role = FOLLOWER
			rf.VotedFor = args.CandidateID
			rf.CurrentTerm = args.Term
			rf.persist()
			rf.lastHeartbeatUnixTime = time.Now().UnixNano()
			electionDebug(fmt.Sprintf("Peer %d voted for Peer %d::%v, %v, %v, 2. Logs: %v", rf.me, args.CandidateID,
				len(rf.Logs) == 1, args.LastLogTerm > rf.Logs[len(rf.Logs)-1].Term,
				args.LastLogTerm == rf.Logs[len(rf.Logs)-1].Term && args.LastLogIndex >= rf.Logs[len(rf.Logs)-1].Index,
				rf.Logs))
			reply.Term = args.Term
			reply.VoteGranted = true
		}else{
			reply.Term = args.Term
			reply.VoteGranted = false
		}
	}
	rf.mu.Unlock()
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
	// log.Printf("Peer %d received response from peer %d", rf.me, server)
	return ok
}

type AppendEntriesArgs struct{
	// AppendEntries RPCs that carry no log entries is the heartbeat
	Term			int			// Term of current leader
	LeaderID		int			// ID of current leader
	PrevLogIndex	int			// Index of log entry immediately preceding new ones
	PrevLogTerm		int			// Term of prevLogIndex entry
	Entries			[]LogEntry	// Log entries for to store(empty for heartbeat, may send multiple for efficiency
	LeaderCommit	int			// Leader's commit index
	// TODO: add data record about entries. For heartBeat, we only need first two properties.
}

type AppendEntriesReply struct{
	Term			int			// Term of the follower
	Success			bool 		// TODO: used for matching entries
}

func (rf*Raft) followerUpdateCommitIndex(args *AppendEntriesArgs){
	if args.LeaderCommit > rf.committedIndex{
		newCommitted := int(math.Min(float64(args.LeaderCommit), float64(rf.Logs[len(rf.Logs)-1].Index)))
		/* Apply log changes to tester too */
		newLogs := rf.Logs[rf.committedIndex+1:newCommitted+1]
		doChangeCommitted := false
		for _, e := range newLogs{
			/* 	Test2BReJoin: HeartBeat before really distributed some entries may pass a false
			information telling the raft to committed entries that was not existed in the
			current leader's log. For example, a reconnected previous leader may has different log in
			new leader's committedIndex, a heartbeat like this may apply wrong data to state
			machine! , so we need to check the log entry's term */
			if args.Entries == nil && e.Term < rf.CurrentTerm {
				break
			}
			rf.applyMessage2Tester(e)
			doChangeCommitted = true
		}
		if doChangeCommitted{
			/* Here we need to distinguish the heartbeat and non-heartbeat:
			Since the heartbeat is used to exchange information about committed
			log entries, but it may not actually change the committed entries,
			so only change rf.committedIndex when there are new committed log entries
			are apply to tester
			*/
			rf.committedIndex = newCommitted
		}
	}

}

/* Before call this function, the lock of 'rf' should be on! */
func (rf*Raft) doAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	//log.Printf("Peer%d: arg prevlog term=%d, arg prevlog index index=%d, self last log term=%d, self last log " +
	//	"index=%d \nnew entry first index=%d, new entry fisrt term=%d, committed index=%d", rf.me, args.PrevLogTerm,
	//	args.PrevLogIndex, rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index, args.Entries[0].Index,
	//	args.Entries[0].Term, rf.committedIndex)
	replicationDebug(fmt.Sprint("Peer ", rf.me, "  PrevLogTerm:", args.PrevLogTerm, "  PrevLogIndex: ", args.PrevLogIndex,
		"  newEntries: ", args.Entries, "  origin logs: ", rf.Logs))
	if len(rf.Logs) == 1{
		if args.PrevLogIndex == 0{
			reply.Success = true
			rf.Logs = append(rf.Logs, args.Entries...)
			rf.persist()
			rf.followerUpdateCommitIndex(args)
		}else{
			/* If the server has no log but the leader has, then we need to add all
			logs in leader to this server, but we should do it step
			by step by using appendEntriesRPC and nextIndex[] go back */
			reply.Success = false
		}
	}else{
		if rf.Logs[len(rf.Logs)-1].Term == args.PrevLogTerm &&
			rf.Logs[len(rf.Logs)-1].Index < args.PrevLogIndex{
			/* If the server's log is behind the leaders in index, we need to find
			the match point and add all entries after that point step
			by step by using appendEntriesRPC and nextIndex[] go back */
			reply.Success = false
		}else if rf.Logs[len(rf.Logs)-1].Term == args.PrevLogTerm &&
			rf.Logs[len(rf.Logs)-1].Index == args.PrevLogIndex{
			/* Here we can apply new entries to the server. */
			reply.Success = true
			rf.Logs = append(rf.Logs, args.Entries...)
			rf.persist()
			rf.followerUpdateCommitIndex(args)
		}else if rf.Logs[len(rf.Logs)-1].Term == args.PrevLogTerm &&
			rf.Logs[len(rf.Logs)-1].Index > args.PrevLogIndex{
			/* If the server's log exceed the leaders(unlikely), remove all extra log
			entries.*/
			beginReplicatedLoc := -1
			for i := len(rf.Logs)-2; i > 0; i--{
				if rf.Logs[i].Index == args.PrevLogIndex && rf.Logs[i].Term == args.PrevLogTerm{
					beginReplicatedLoc = i
					break
				}
			}
			if beginReplicatedLoc != -1{
				rf.Logs = append(rf.Logs[0: beginReplicatedLoc+1], args.Entries...)
				rf.persist()
				reply.Success = true
				rf.followerUpdateCommitIndex(args)
			}else{
				reply.Success = false
			}
		}else if rf.Logs[len(rf.Logs)-1].Term != args.PrevLogTerm{
			/* If the server's log is behind the leaders in term, we need to find
			the match point and add all entries after that point step
			by step by using appendEntriesRPC and nextIndex[] go back. */
			if args.PrevLogTerm == -1{
				/* This means that the follower should apply all new entries to
				its Logs. */
				rf.Logs = append(rf.Logs[0:1], args.Entries...)
				rf.persist()
				reply.Success = true
				rf.followerUpdateCommitIndex(args)
			}else{
				reply.Success = false
			}
			/* TODO: However, it is possible that the server has no common log
			entries with leader(though it is very unlikely.) In this situation, need to
			set a special method to copy whole log to the server */
		}else{
			/* Should not enter here !!!*/
			log.Printf("Enter the wrong branch when append entries!")
			reply.Success = false
		}
	}
	//log.Printf("After append entries, the logs of peer %d is %d",
		//rf.me, rf.logs)
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	if rf.killed(){
		return
	}
	rf.mu.Lock()
	if rf.role != FOLLOWER {
		if args.Term >= rf.CurrentTerm {
			rf.role = FOLLOWER
			rf.VotedFor = args.LeaderID
			rf.CurrentTerm = args.Term
			rf.persist()
			rf.lastHeartbeatUnixTime = time.Now().UnixNano()
			reply.Term = rf.CurrentTerm
			reply.Success = true
			/* Not a heartbeat */
			if args.Entries != nil{
				rf.doAppendEntries(args, reply)
			}else{
				/* Heartbeat  */
				if rf.Logs[len(rf.Logs)-1].Index <= args.PrevLogIndex{
					reply.Success = false
				}else {
					rf.followerUpdateCommitIndex(args)
				}
			}
			//log.Printf("Peer %d receive heartbeat from Peer %d", rf.me, args.LeaderID)
		} else {
			reply.Term = rf.CurrentTerm
			reply.Success = false
			//log.Printf("Peer %d reject heartbeat from Peer %d", rf.me, args.LeaderID)
		}
	}else{
		if args.Term >= rf.CurrentTerm {
			rf.lastHeartbeatUnixTime = time.Now().UnixNano()
			rf.VotedFor = args.LeaderID
			rf.CurrentTerm = args.Term
			rf.persist()
			reply.Term = args.Term
			reply.Success = true
			if args.Entries != nil{
				rf.doAppendEntries(args, reply)
			}else{
				if rf.Logs[len(rf.Logs)-1].Index <= args.PrevLogIndex{
					reply.Success = false
				}else {
					rf.followerUpdateCommitIndex(args)
				}
			}
			//log.Printf("Peer %d receive heartbeat from Peer %d", rf.me, args.LeaderID)
		}else{
			reply.Term = rf.CurrentTerm
			reply.Success = false
			//log.Printf("Peer %d reject heartbeat from Peer %d", rf.me, args.LeaderID)
		}
		//// log.Printf("Peer %d receive heartbeat from Peer %d", rf.me, args.LeaderID)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


func (rf *Raft) applyMessage2Tester(logEntry LogEntry){
	msg := ApplyMsg{}
	msg.CommandIndex = logEntry.Index
	msg.Command = logEntry.Info
	msg.CommandValid = true
	replicationDebug(fmt.Sprintf("Peer%d(%d) apply log%d with command %d to tester", rf.me, rf.role,
		logEntry.Index,	logEntry.Info))
	rf.applyChan <- msg
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
	rf.mu.Lock()
	isLeader := rf.role == LEADER
	// Your code here (2B).
	if len(rf.Logs) == 1{
		index = 1
	}else{
		index = rf.Logs[len(rf.Logs)-1].Index + 1
	}
	term = rf.CurrentTerm
	if isLeader{
		// Index start from 1
		newLogEntry := LogEntry{term, index, command}
		rf.Logs = append(rf.Logs, newLogEntry)
		rf.persist()
		for rf.distributeAppendEntries(false) != 1{
			if rf.killed(){
				break
			}
			time.Sleep(time.Millisecond * 10)
		}
		rf.mu.Unlock()
		replicationDebug(fmt.Sprintf("Leader%d return index:%d, term:%d", rf.me, index, term))
	}else{
		rf.mu.Unlock()
		replicationDebug(fmt.Sprintf("Peer%d return index:%d, term:%d", rf.me, index, term))
	}

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

type AppendEntriesCallResponse struct{
	Success 		int		/* 0 means success, integer greater than 0 represent
								failed , -1 means server lost connection */
	ID				int		/* ID of the response server */
}

func (rf *Raft) leaderUpdateCommitIndex(){
	commitIndexMap := make(map[int]int)
	for i := 0; i < len(rf.peers); i++{
		if i == rf.me{
			continue
		}
		if v, ok:= commitIndexMap[rf.matchIndex[i]]; ok{
			commitIndexMap[rf.matchIndex[i]] = v + 1
		}else{
			commitIndexMap[rf.matchIndex[i]] = 2
		}
	}
	for k, v := range commitIndexMap{
		if v > (len(rf.peers) - 1)/2{
			if k > rf.committedIndex && rf.Logs[k].Term == rf.CurrentTerm {
				/* Apply the committed log to tester*/
				newLogs := rf.Logs[rf.committedIndex+1:k+1]
				for _, l := range newLogs{
					rf.applyMessage2Tester(l)
				}
				rf.committedIndex = k
				replicationDebug(fmt.Sprintf("Leader%d's nextIndex[]:%d, matchIndex[]: %d", rf.me,
					rf.nextIndex, rf.matchIndex))
				//for i := 0; i < len(rf.peers); i++{
				//	if i == rf.me{
				//		continue
				//	}
				//	rf.nextIndex[i] = rf.committedIndex + 1
				//}
			}
			break
		}
	}
}

/* return whether the appendEntries for all servers succeed, if not return false,
 *and inform the leader to distribute again. If it is a heartbeat, then always return true.
 * Return 0 means retry, return 1 means ok, and 2 means turn heartbeat to append entries
 */
func (rf *Raft) distributeAppendEntries(heartbeat bool) int {
	responseChan := make(chan AppendEntriesCallResponse, len(rf.peers))
	ret := 1
	if heartbeat{
		rf.mu.Lock()
	}
	for i := 0; i < len(rf.peers)&&!rf.killed(); i++{
		if i == rf.me{
			continue
		}
		//log.Printf("Peer %d send heartbeat to Peer %d", rf.me, i)

		args := AppendEntriesArgs{
			rf.CurrentTerm,
			rf.me,
			-1,
			-1,
			nil,
			rf.committedIndex}
		appendEntriesReply := AppendEntriesReply{}
		if rf.nextIndex[i] <= rf.Logs[len(rf.Logs)-1].Index{
			if rf.nextIndex[i] > 0{
				args.PrevLogIndex = rf.Logs[rf.nextIndex[i]-1].Index
				args.PrevLogTerm = rf.Logs[rf.nextIndex[i]-1].Term
			}else{
				rf.nextIndex[i] = 1
				args.PrevLogIndex = 0
				args.PrevLogTerm = rf.Logs[0].Term
			}
		}
		if heartbeat{
			args.Entries = nil
		}else{
			if rf.nextIndex[i] <= rf.Logs[len(rf.Logs)-1].Index{
				args.Entries = rf.Logs[rf.nextIndex[i]: len(rf.Logs)]
			}else{
				/* If enter this branch means there are some servers but not this one
				reject the leader or lost connection with the leader, then here
				send a heartbeat to this server is enough since it doesn't need to apply
				new logs */
				args.Entries = nil
			}
		}
		go func(reChan chan AppendEntriesCallResponse, followerId int, sender *Raft, appendArg *AppendEntriesArgs,
			reply *AppendEntriesReply){
			if !sender.sendAppendEntries(followerId, appendArg, reply){
				reChan <- AppendEntriesCallResponse{-1, followerId}
				return
			}
			if reply.Success{
				reChan <- AppendEntriesCallResponse{0, followerId}
			}else{
				reChan <- AppendEntriesCallResponse{reply.Term, followerId}
			}
		}(responseChan, i, rf, &args, &appendEntriesReply)
	}
	notALeader := false
	/* If normal append entries rpc didn't reach one of the servers, the
	leader will retry to send the append entries rpc until all */
	for round:=1; round < len(rf.peers) && !notALeader; round++{
		var val AppendEntriesCallResponse
		select {
			case val = <-responseChan:
				if heartbeat{

					if val.Success > 0 && rf.CurrentTerm < val.Success{
						rf.CurrentTerm = val.Success
						rf.persist()
						rf.role = FOLLOWER
						notALeader = true
					}else if val.Success > 0 && rf.CurrentTerm == val.Success{
						/* If terms of leader and the follower are the same, then
						the only reason why the follower reject the appendEntriesRPC
						is that the term of prevLogIndex in the follower doesn't match
						prevLogTerm */
						ret = 2
						//log.Printf("Peer%d rejet heartbeat from leader%d", val.ID,
						//	rf.me)
					}else if val.Success < 0{
						//log.Printf( "Leader%d think peer%d lost connection", rf.me,
							//val.ID)
					}
				}else{
					if val.Success > 0 && rf.CurrentTerm < val.Success{
						rf.CurrentTerm = val.Success
						rf.persist()
						rf.role = FOLLOWER
						notALeader = true
					}else if val.Success > 0 && rf.CurrentTerm == val.Success{
						/* If terms of leader and the follower are the same, then
						the only reason why the follower reject the appendEntriesRPC
						is that the term of prevLogIndex in the follower doesn't match
						prevLogTerm */
						delta := rf.Logs[len(rf.Logs)-1].Index - rf.nextIndex[val.ID]
						if delta < 5 {
							rf.nextIndex[val.ID] -= 1
						}else if delta >= 5 && delta <=20{
							rf.nextIndex[val.ID] -= 5
						}else{
							rf.nextIndex[val.ID] -= delta
						}
						if rf.nextIndex[val.ID] <= 0{
							rf.nextIndex[val.ID] = 1
						}
						replicationDebug(fmt.Sprintf("Peer%d failed to get new log entry, decrease nextIndex to% d",
							val.ID, rf.nextIndex[val.ID]))
						ret = 0
					}else if val.Success == 0{
						rf.nextIndex[val.ID] = rf.Logs[len(rf.Logs)-1].Index + 1
						rf.matchIndex[val.ID] = rf.nextIndex[val.ID] - 1
					}else{
						/* lost connection with val.ID or server didn't reply in time */
						//log.Printf( "Leader%d think peer%d lost connection", rf.me,
							//val.ID)
						ret = 0
					}
				}
				break
			case <-time.After(time.Millisecond*time.Duration(heartbeatCircle/int64(len(rf.peers) + 2))):
				/* lost connection with val.ID or server didn't reply in time */
				/* TODO: Under this situation, does the leader need to retry send
				AppendEntriesRPC */
				break
		}
	}
	rf.leaderUpdateCommitIndex()
	if heartbeat{
		rf.mu.Unlock()
	}
	if ret == 0 && !heartbeat{
		//log.Printf("Failed to distributed log entires, try again!")
		return ret
	}
	return ret
	//// log.Printf("Peer %d finish a round of send heartbeat", rf.me)
}

func (rf *Raft) startElection(){
	// Here we should start a new election
	// Set a random election timeout to avoid split vote.
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	rf.persist()
	rf.lastHeartbeatUnixTime = time.Now().UnixNano()
	requestArg := RequestVoteArgs{
		rf.CurrentTerm,
		rf.me,
		rf.Logs[len(rf.Logs)-1].Index,
		rf.Logs[len(rf.Logs)-1].Term}
	// Begin to send RequestVote
	rf.mu.Unlock()
	voteChan := make(chan int, len(rf.peers))

	// send one vote request in one goroutine
	for i := 0; i < len(rf.peers) && !rf.killed() && rf.me == rf.GetVoteForLocked(); i++ {
		requestReply := RequestVoteReply{}
		if i == rf.me{
			continue
		}
		if rf.killed(){
			break
		}
		//log.Printf("Peer %d request vote from peer %d", rf.me, i)
		// First vote for self, 1 means votes
		go func(voterId int, voteCh chan int, sender *Raft, args *RequestVoteArgs, reply *RequestVoteReply){
			if !sender.sendRequestVote(voterId, args, reply){
				// do not reply means do not vote
				voteCh <- -1
			}else{
				if requestReply.VoteGranted{
					voteCh <- -2
				}else{
					voteCh <- requestReply.Term
				}
			}
		}(i, voteChan, rf, &requestArg, &requestReply)
	}

	// Wait for the vote result, if timeout then the peer treat the other peer didn't vote for it.
	beChosen 	:= 0
	counter 	:= 1
	votes		:= 1	// one votes for itself
	for round:=1; round < len(rf.peers); round++ {
		// log.Printf("Peer %d wait for votes result", rf.me)
		val := 0
		select{
		case val = <-voteChan:
			if val == -2{
				votes += 1
			}else if val == -1{
				// has no reply from voter
			}else{
				rf.mu.Lock()
				if val > rf.CurrentTerm {
					rf.CurrentTerm = val
					rf.persist()
					rf.role = FOLLOWER
					rf.mu.Unlock()
					break
				}
				rf.mu.Unlock()
			}
			counter += 1
			break
		case <-time.After(time.Millisecond* 20):
			counter += 1
			break
		}
		//log.Printf("Peer %d receive %d votes", rf.me, votes)
		if votes > len(rf.peers)/2{
			beChosen = votes
			break
		}
	}
	rf.mu.Lock()
	if beChosen != 0{
		rf.role = LEADER
		/* Before send heartbeat, set all properties of the leader correctly */
		rf.turnToLeader()
		/* Here the appendEntriesArgs are set for heartbeat */
		electionDebug(fmt.Sprintf("Peer %d become leader, received %d votes, the term is %d. Logs is  %v", rf.me, beChosen,
			rf.CurrentTerm, rf.Logs))
		rf.mu.Unlock()
		if rf.killed(){
			return
		}
		/* Send heartbeat twice since we need to wait the timeout of election */

		rf.distributeAppendEntries( true)
		//log.Printf("Peer %d is leader and finish first round of heartbeat!",
			//rf.me)
	}else{
		electionDebug(fmt.Sprintf("Peer %d received %d votes and failed!", rf.me, votes))
		rf.role = FOLLOWER
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds.
// Such a range only makes sense if the leader sends heartbeats considerably more often than once
// per 150 milliseconds. Because the tester limits you to 10 heartbeats per second, you will have to
// use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large,
// because then you may fail to elect a leader within five seconds.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.role == LEADER{
			/* Here send heartbeat, since no entry is appended. */
			rf.mu.Unlock()
			if rf.killed(){
				return
			}
			ret := rf.distributeAppendEntries( true)
			if ret == 2{
				for ret == 2{
					rf.mu.Lock()
					ret = rf.distributeAppendEntries(false)
					rf.mu.Unlock()
					time.Sleep(time.Millisecond * 10)
				}
			}
			time.Sleep(time.Millisecond * time.Duration(int(heartbeatCircle)/len(rf.peers)))
		}else if rf.role == FOLLOWER{
			curSecond := time.Now().UnixNano()
			difference := curSecond - rf.lastHeartbeatUnixTime
			electionTimeout := electionTimeoutLower + rand.Intn(electionTimeoutGap)
			if difference/(1000*1000) >= int64(electionTimeout){
				//log.Printf("The time difference for  peer %d is %d", rf.me, difference)
				// Didn't hear from the leader for a 'long time', begin to start a new election
				rf.VotedFor = -1
				rf.persist()
				rf.mu.Unlock()
				electionDebug(fmt.Sprintf("Peer %d ready for election.\n", rf.me))
				time.Sleep(time.Millisecond * time.Duration(electionTimeout))
				rf.mu.Lock()
				if rf.killed() || rf.lastHeartbeatUnixTime >= curSecond || rf.VotedFor != -1{
					electionDebug(fmt.Sprintf("Peer %d give up election.\n", rf.me))
					rf.mu.Unlock()
					time.Sleep(time.Millisecond * time.Duration(electionTimeout))
					continue
				}
				rf.mu.Unlock()
				rf.startElection()
				if rf.killed(){
					break
				}
				rf.mu.Lock()
				if rf.role != LEADER{
					rf.VotedFor = -1
					rf.persist()
					rf.mu.Unlock()
					time.Sleep(time.Millisecond * time.Duration(electionTimeout))
				}else{
					rf.mu.Unlock()
				}
			}else{
				rf.mu.Unlock()
				time.Sleep(time.Millisecond * time.Duration(electionTimeout))
				//time.Sleep(time.Millisecond * time.Duration(rand.Intn(int(heartbeatCircle)) + 1))
			}

		}
	}
	//// log.Printf("Peer %d is killed.", rf.me)
	return
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
	rf.applyChan = applyCh

	// Update on stable storage before responding to RPC
	rf.lastApplied = 0
	rf.committedIndex = 0
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	// Set default size of logs to 1000
	rf.Logs = make([]LogEntry, 0, defaultLogCapacity)
	// Add an empty log entry for holding the 0-index place
	rf.Logs = append(rf.Logs, LogEntry{-1, 0 , -1})
	initApplyMsg := ApplyMsg{}
	initApplyMsg.Command = -1
	initApplyMsg.CommandValid = true
	initApplyMsg.CommandIndex = 0
	rf.applyChan <- initApplyMsg

	// Reinitialized after election, only held by leaders.
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Other properties
	rf.lastHeartbeatUnixTime = 0
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// log.Printf("Successfully make peer %d.\n", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
