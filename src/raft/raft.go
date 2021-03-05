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
var heartbeatCircle int64 = 120
// Election timeout, ms.
var electionTimeoutLower = 200
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


	lastHeartbeatUnixTime		int64			// Record for the system time of last heartbeat (unix time nano)

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) GetVoteForLocked() int{
	ret := 0
	rf.mu.Lock()
	ret = rf.votedFor
	rf.mu.Unlock()
	return ret
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	term = rf.currentTerm
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
	if rf.killed(){
		return
	}
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
			rf.role = FOLLOWER
			rf.currentTerm = args.Term
			rf.lastHeartbeatUnixTime = time.Now().UnixNano()
			reply.Term = args.Term
			reply.VoteGranted = true
			//log.Printf("Peer %d voted for Peer %d", rf.me, args.CandidateID)
		}
	}else{
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.role = FOLLOWER
		rf.currentTerm = args.Term
		rf.lastHeartbeatUnixTime = time.Now().UnixNano()
		//log.Printf("Peer %d voted for Peer %d", rf.me, args.CandidateID)
		reply.Term = args.Term
		reply.VoteGranted = true
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
	// log.Printf("Peer %d received response from peer %d", rf.me, server)
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
	if rf.killed(){
		return
	}
	rf.mu.Lock()
	if rf.role != FOLLOWER {
		if args.Term >= rf.currentTerm {
			rf.role = FOLLOWER
			rf.currentTerm = args.Term
			rf.lastHeartbeatUnixTime = time.Now().UnixNano()
			reply.Term = rf.currentTerm
			reply.Success = true
			//log.Printf("Peer %d receive heartbeat from Peer %d", rf.me, args.LeaderID)
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
			//log.Printf("Peer %d reject heartbeat from Peer %d", rf.me, args.LeaderID)
		}
	}else{
		if args.Term >= rf.currentTerm{
			rf.lastHeartbeatUnixTime = time.Now().UnixNano()
			rf.currentTerm = args.Term
			reply.Term = args.Term
			reply.Success = true
			//log.Printf("Peer %d receive heartbeat from Peer %d", rf.me, args.LeaderID)
		}else{
			reply.Term = rf.currentTerm
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
// The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds.
// Such a range only makes sense if the leader sends heartbeats considerably more often than once
// per 150 milliseconds. Because the tester limits you to 10 heartbeats per second, you will have to
// use an election timeout larger than the paper's 150 to 300 milliseconds, but not too large,
// because then you may fail to elect a leader within five seconds.

func (rf *Raft) distributeAppendEntries(args *AppendEntriesArgs){
	responseChan := make(chan int, len(rf.peers))
	for i := 0; i < len(rf.peers)&&!rf.killed(); i++{
		appendEntriesReply := AppendEntriesReply{}
		if i == rf.me{
			continue
		}
		//// log.Printf("Peer %d send heartbeat to Peer %d", rf.me, i)
		go func(reChan chan int, followerId int, sender *Raft, appendArg *AppendEntriesArgs,
			reply *AppendEntriesReply){
			if !sender.sendAppendEntries(followerId, appendArg, reply){
				reChan <- -1
				return
			}
			if reply.Success{
				reChan <- 0
			}else{
				reChan <- reply.Term
			}
		}(responseChan, i, rf, args, &appendEntriesReply)
		// TODO: handle failed reply when add log function in 2B
	}
	notALeader := false
	for round:=1; round < len(rf.peers) && !notALeader; round++{
		val := 0
		select {
		case val = <-responseChan:
			rf.mu.Lock()
			if val != 0 && rf.currentTerm < val{
				rf.currentTerm = val
				rf.role = FOLLOWER
				notALeader = true
			}
			rf.mu.Unlock()
			break
		case <-time.After(time.Millisecond*time.Duration(heartbeatCircle/int64(len(rf.peers) + 2))):
			break
		}
	}
	//// log.Printf("Peer %d finish a round of send heartbeat", rf.me)
}

func (rf *Raft) startElection(){
	// Here we should start a new election
	// Set a random election timeout to avoid split vote.
	rf.mu.Lock()
	rf.role = CANDIDATE
	rf.currentTerm += 1
	rf.lastHeartbeatUnixTime = time.Now().UnixNano()
	requestArg := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		0,
		0}
	// Begin to send RequestVote
	rf.votedFor = rf.me
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
				voteCh <- 2
			}
			if requestReply.VoteGranted{
				voteCh <- 1
			}else{
				voteCh <- 2
			}
		}(i, voteChan, rf, &requestArg, &requestReply)
	}

	// Wait for the vote result, if timeout then the peer treat the other peer didn't vote for it.
	beChosen 	:= 0
	counter 	:= 1
	votes		:= 1	// one votes for itself
	for round:=0; round < len(rf.peers) &&  counter < len(rf.peers); round++ {
		// log.Printf("Peer %d wait for votes result", rf.me)
		val := 0
		select{
		case val = <-voteChan:
			if val == 1{
				votes += 1
			}
			counter += 1
			break
		case <-time.After(time.Millisecond*time.Duration(heartbeatCircle/int64(len(rf.peers)))):
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
		//log.Printf("Peer %d become leader, received %d votes, the term is %d", rf.me, beChosen,
			//rf.currentTerm)
		appendEntriesArgs := AppendEntriesArgs{rf.currentTerm, rf.me}
		rf.mu.Unlock()
		if rf.killed(){
			return
		}
		rf.distributeAppendEntries(&appendEntriesArgs)
		//log.Printf("Peer %d is leader and finish first round of heartbeat!",
			//rf.me)
	}else{
		//log.Printf("Peer %d received %d votes and failed!", rf.me, votes)
		rf.role = FOLLOWER
		rf.mu.Unlock()
	}
}


func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.role == LEADER{
			appendEntriesArgs := AppendEntriesArgs{rf.currentTerm, rf.me}
			rf.mu.Unlock()
			if rf.killed(){
				return
			}
			rf.distributeAppendEntries(&appendEntriesArgs)
			//time.Sleep(time.Millisecond * time.Duration(rand.Intn(150) + 1))
		}else if rf.role == FOLLOWER{
			curSecond := time.Now().UnixNano()
			difference := curSecond - rf.lastHeartbeatUnixTime
			if difference/(1000*1000) >= heartbeatCircle{
				//log.Printf("The time difference for  peer %d is %d", rf.me, difference)
				// Didn't hear from the leader for a 'long time', begin to start a new election
				rf.votedFor = -1
				rf.mu.Unlock()
				//log.Printf("Peer %d ready for election.\n", rf.me)
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(int(heartbeatCircle+1))+electionTimeoutLower))
				rf.mu.Lock()
				if rf.killed() || rf.lastHeartbeatUnixTime >= curSecond || rf.votedFor != -1{
					//log.Printf("Peer %d give up election.\n", rf.me)
					rf.mu.Unlock()
					time.Sleep(time.Millisecond * time.Duration(int(heartbeatCircle)))
					continue
				}
				rf.mu.Unlock()
				rf.startElection()
				if rf.killed(){
					break
				}
			}else{
				rf.mu.Unlock()
				time.Sleep(time.Millisecond * time.Duration(int(heartbeatCircle)))
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
	rf.lastHeartbeatUnixTime = 0
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// log.Printf("Successfully make peer %d.\n", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
