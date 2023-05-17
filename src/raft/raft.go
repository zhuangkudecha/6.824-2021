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
	//	"bytes"
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// volatile state on all servers
	cimmitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	// electionTimeout int // electionTime out for followers
	// volatile state on leaders
	nextIdexs   []int //初始化为最后一个log 的index+1
	matchIndexs []int //初始化为0 server 中已经成功aplied 的log 的index

	// all server election states
	role           string
	leaderIndex    int
	lastActiveTime time.Time // reset when:1.recevie heartbeat from leader
	timeouts       time.Duration
	// 2. vote for other candidate
	// 3. request vote from others

	lastBroadcastTime time.Time // as leader , last broadCast time

	applyCh chan ApplyMsg // application chan

	moreApply bool
	applyCond *sync.Cond

	// for lab2D
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// current role
const ROLE_LEADER = "Leader"
const ROLE_FOLLOW = "Follower"
const ROLE_CANDIDATE = "Candidate"

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.role == ROLE_LEADER)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)

	// start index is the first index of contained log
	// if log is empty, start index is last included index
	// if log is not empty, start index is the first index of log (the last included index + 1)
	// if len(rf.log) == 0 && rf.lastIncludedIndex == -1 {
	// 	rf.startIndex = 0
	// } else {
	// 	rf.startIndex = rf.lastIncludedIndex + 1
	// }

	data := w.Bytes()
	Debug(dPersist, "S%d Persist lastIncludedIndex=%d", rf.me, rf.lastIncludedIndex)
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
	d.Decode(&rf.lastIncludedIndex)
	d.Decode(&rf.lastIncludedTerm)
	Debug(dPersist, "S%d read from Persist lastIncludedIndex=%d", rf.me, rf.lastIncludedIndex)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	// rf.mu.Lock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	// rf.mu.Unlock()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d start CondInstallSnapshot", rf.me)
	// the snapshot is outdated refuse to install
	if lastIncludedIndex <= rf.cimmitIndex {
		return false
	}
	defer func() {
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		// rf.s = snapshot
		rf.cimmitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.snapshot = snapshot
		Debug(dSnap, "S%d persist state and snapshot", rf.me)
		rf.persistStateAndSnapshot(snapshot)
	}()

	// the log contains the snapshot trim the outdated log
	if lastIncludedIndex < rf.lastIncludedIndex+len(rf.log) && lastIncludedTerm == rf.lastIncludedTerm {
		rf.log = append([]LogEntry(nil), rf.log[lastIncludedIndex-rf.lastIncludedIndex:]...)
		return true
	}

	// the shapshot is more up-to-date than the log trim the whole log
	rf.log = make([]LogEntry, 0)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dSnap, "S%d start snapshot index %v", rf.me, index)
	if index <= rf.lastIncludedIndex {
		// index is too small, there is no need to snapshot
		Debug(dSnap, "S%d snapshot index %v is too small, there is no need to snapshot", rf.me, index)
		return
	}

	rf.lastIncludedTerm = rf.log[index-rf.lastIncludedIndex-1].Term
	rf.log = append([]LogEntry(nil), rf.log[index-rf.lastIncludedIndex:]...)
	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	rf.persistStateAndSnapshot(snapshot)
	Debug(dSnap, "S%d snapshot index %v, lastIncludedIndex %v, lastIncludedTerm %v done", rf.me, index, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

type InstallSnapshotArgs struct {
	Term              int    //leader's term
	LeaderId          int    //
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastincludedIndex
	Snapshot          []byte // raw bytes of the snapshot chunk starting
}

type InstallSnapshotReply struct {
	Term int //currentTerm for leader tot update itself
}

func (rf *Raft) InstallSnapshot(arg *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	switch {
	case arg.Term < rf.currentTerm:
		Debug(dSnap, "S%d [InstallSnapshot] term %v < currentTerm %v, reply immediately", rf.me, arg.Term, rf.currentTerm)
		return

	case arg.Term > rf.currentTerm:
		rf.currentTerm = arg.Term
		rf.persist()
		if rf.role != ROLE_FOLLOW {
			rf.role = ROLE_FOLLOW
		}

	case arg.Term == rf.currentTerm:
		if rf.role == ROLE_LEADER {
			Debug(dError, "S%d get snapshot from another leader S%d", rf.me, arg.LeaderId)
		} else if rf.role == ROLE_CANDIDATE {
			rf.role = ROLE_FOLLOW
		}
	}
	if arg.LastIncludedIndex <= rf.lastIncludedIndex {
		Debug(dSnap, "S%d [InstallSnapshot] arg.LastIncludedIndex %v <= rf.lastIncludedIndex %v, discard any existing or partial snapshot with a smaller index", rf.me, arg.LastIncludedIndex, rf.lastIncludedIndex)
		return
	}
	rf.lastActiveTime = time.Now()
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      arg.Snapshot,
		SnapshotTerm:  arg.LastIncludedTerm,
		SnapshotIndex: arg.LastIncludedIndex,
	}
	go func() {
		rf.applyCh <- applyMsg
	}()
	// reply immediately if term < currentTerm
	// if arg.Term < rf.currentTerm {
	// 	Debug(dSnap, "S%d [InstallSnapshot] term %v < currentTerm %v, reply immediately", rf.me, arg.Term, rf.currentTerm)
	// 	return
	// }
	// // discard snapshot with a smaller index
	// if arg.LastIncludedIndex <= rf.lastIncludedIndex {
	// 	Debug(dSnap, "S%d [InstallSnapshot] arg.LastIncludedIndex %v <= rf.lastIncludedIndex %v, discard any existing or partial snapshot with a smaller index", rf.me, arg.LastIncludedIndex, rf.lastIncludedIndex)
	// 	return
	// }

	// Debug(dSnap, "S%d [InstallSnapshot] reset state machine using snapshot contents, rf.lastIncludedIndex %v", rf.me, rf.lastIncludedIndex)

	// // send snapshot to applyCh. whether need to use goroutine?
	// applyMsg := ApplyMsg{
	// 	SnapshotValid: true,
	// 	Snapshot:      arg.Snapshot,
	// 	SnapshotTerm:  arg.LastIncludedTerm,
	// 	SnapshotIndex: arg.LastIncludedIndex,
	// }
	// go func() {
	// 	rf.applyCh <- applyMsg
	// }()
}

func (rf *Raft) sendSnapshot(id int, args InstallSnapshotArgs, currentTerm int) {

	reply := InstallSnapshotReply{}
	ok := rf.peers[id].Call("Raft.InstallSnapshot", &args, &reply)

	if ok && !rf.killed() {
		Debug(dSnap, "S%d [sendSnapshot] to S%d get reply", rf.me, id)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.currentTerm != currentTerm || rf.role != ROLE_LEADER || reply.Term > rf.currentTerm {
			Debug(dSnap, "S%d sendSnapshot to S%d but failed", rf.me, id)
			if reply.Term > rf.currentTerm {
				rf.role = ROLE_FOLLOW
				rf.currentTerm = reply.Term
				rf.persist()
				rf.lastActiveTime = time.Now()
			}
			return
		}
		Debug(dSnap, "S%d sendSnapshot to S%d success", rf.me, id)
		rf.nextIdexs[id] = args.LastIncludedIndex + 1
		rf.matchIndexs[id] = rf.lastIncludedIndex

	}

	// if reply.Term > rf.currentTerm {
	// 	rf.role = ROLE_FOLLOW
	// 	return
	// }
	// if !ok {
	// 	return
	// }
	// rf.nextIdexs[id] = rf.lastIncludedIndex + 1
	// rf.matchIndexs[id] = rf.lastIncludedIndex
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log enry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	Debug(dClient, "S%d Handle RequestVote, CandidatesId%d Term%d CurrentTerm%d LastLogIndex%d LastLogTerm%d votedFor%d",
		rf.me, args.CandidateId, args.Term, rf.currentTerm, args.LastLogIndex, args.LastLogTerm, rf.votedFor)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		// 如果请求任期 小于当前 或者 term 相等但是已经投过票了 拒绝给他投票
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		if args.Term < rf.currentTerm {
			Debug(dClient, "S%d Refuse vote S%d cause term is older than me", rf.me, args.CandidateId)

		}
		if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			Debug(dClient, "S%d Refuse vote S%d cause already vote for someone else", rf.me, args.CandidateId)
		}
		return
	}
	if args.Term > rf.currentTerm {
		// 请求的任期大于当前任期 转变为follower 重新投票
		rf.role = ROLE_FOLLOW
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// 还需要判断log是否是最新的
	// 1. 当前node 的log index = 0
	// 2. 请求的最后一个index的term 大于当前node 的最后一个index 的term

	myLastLogIndex := rf.lastIncludedIndex + len(rf.log)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// whether the candidate's log is at least as up-to-date as receiver's log
		// 1. reviver's log is empty (the myLastLogIndex < 0)
		// 2. the candidate's last log term is greater than the receiver's last log term
		// 3. the candidate's last log term is equal to the receiver's last log term
		// and the candidate's last log index is greater than the receiver's last log index
		Debug(dClient, "S%d myStartIndex=%d mylastLogIndex=%d", rf.me, rf.lastIncludedIndex+1, rf.lastIncludedIndex+len(rf.log))

		if len(rf.log) == 0 {
			if myLastLogIndex == 0 || args.LastLogTerm > rf.lastIncludedTerm || (args.LastLogTerm == rf.lastIncludedTerm && args.LastLogIndex >= rf.lastIncludedIndex) {
				rf.votedFor = args.CandidateId
				rf.lastActiveTime = time.Now()
				reply.VoteGranted = true
				rf.role = ROLE_FOLLOW
				Debug(dClient, "S%d voted to S%d", rf.me, args.CandidateId)
				rf.persist()
				return
			}
		} else {
			if myLastLogIndex == 0 || args.LastLogTerm > rf.log[myLastLogIndex-rf.lastIncludedIndex-1].Term || (args.LastLogTerm == rf.log[myLastLogIndex-rf.lastIncludedIndex-1].Term && args.LastLogIndex >= myLastLogIndex) {
				rf.votedFor = args.CandidateId
				rf.lastActiveTime = time.Now()
				reply.VoteGranted = true
				rf.role = ROLE_FOLLOW
				Debug(dClient, "S%d voted to S%d", rf.me, args.CandidateId)
				rf.persist()
				return
			}
		}
	}
	reply.VoteGranted = false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// args for appendentries hander
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReplay struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
	ConflictLen   int
}

func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, reply *AppendEntriesReplay) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Debug(dLog, "S%d Handle AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s]",
		rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role)
	defer func() {
		Debug(dLog, "S%d Return AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s]",
			rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role)
		reply.Term = rf.currentTerm
	}()
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	switch {
	case arg.Term < rf.currentTerm:
		Debug(dError, "S%d currentTerm %d got a outdated AppendEntries from S%d term %d ", rf.me, rf.currentTerm, arg.LeaderId, arg.Term)
		reply.Success = false
		return

	case arg.Term > rf.currentTerm:
		Debug(dError, "S%d currentTerm %d are outdated by S%d term %d, convert to follower", rf.me, rf.currentTerm, arg.LeaderId, arg.Term)
		rf.currentTerm = arg.Term
		rf.persist()

		if rf.role != ROLE_FOLLOW {
			rf.role = ROLE_FOLLOW
		}

	case arg.Term == rf.currentTerm:
		// normal
		if rf.role == ROLE_CANDIDATE {
			rf.role = ROLE_FOLLOW
		}
	}

	if arg.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		Debug(dError, "S%d Reject AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s] because arg.PrevLogIndex %d < rf.lastIncludedIndex %d", rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role, arg.PrevLogIndex, rf.lastIncludedIndex)
		return
	}

	rf.lastActiveTime = time.Now()

	if arg.PrevLogIndex > 0 && ((arg.PrevLogIndex > (rf.lastIncludedIndex + len(rf.log))) || (arg.PrevLogIndex == rf.lastIncludedIndex && rf.lastIncludedTerm != arg.PrevLogTerm) || (arg.PrevLogIndex > rf.lastIncludedIndex && rf.log[arg.PrevLogIndex-rf.lastIncludedIndex-1].Term != arg.PrevLogTerm)) {
		Debug(dError, "S%d lastIncludeLogIndex %d log dismatch with S%d agrs.PrevLogIndex %d ", rf.me, rf.lastIncludedIndex, arg.LeaderId, arg.PrevLogIndex)
		reply.Success = false

		if arg.PrevLogIndex > (rf.lastIncludedIndex + len(rf.log)) {
			reply.ConflictIndex = rf.lastIncludedIndex + 1
			reply.ConflictTerm = -1
			Debug(dError, "S%d Reject AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s] because arg.PrevLogIndex %d > rf.lastIncludedIndex %d + len(rf.log) %d", rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role, arg.PrevLogIndex, rf.lastIncludedIndex, len(rf.log))
			return
		}
		if rf.lastIncludedIndex == arg.PrevLogIndex {
			reply.ConflictTerm = rf.lastIncludedTerm
			reply.ConflictIndex = rf.lastIncludedIndex
			Debug(dError, "S%d lastIncludeLogIndex %d lastIncludedTerm %d mismathch with argprevLogIndex %d argprevLogTerm %d ", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, arg.PrevLogIndex, arg.PrevLogTerm)
			return
		}
		if rf.log[arg.PrevLogIndex-rf.lastIncludedIndex-1].Term != arg.PrevLogTerm {
			reply.ConflictTerm = rf.log[arg.PrevLogIndex-rf.lastIncludedIndex-1].Term
			conflictTermIndex := arg.PrevLogIndex
			// find the first log index with the conflict term
			for conflictTermIndex > rf.lastIncludedIndex && rf.log[conflictTermIndex-rf.lastIncludedIndex-1].Term == reply.ConflictTerm {
				conflictTermIndex--
			}
			reply.ConflictIndex = conflictTermIndex + 1
			Debug(dError, "S%d Reject AppendEntries, LeaderId%d PrevgTerm%d role=[%s] because ConfilcitIndex%d mismatch with mylogTerm", rf.me, arg.LeaderId, arg.PrevLogTerm, rf.role, reply.ConflictIndex, rf.log[reply.ConflictIndex-rf.lastIncludedIndex-1].Term)
			return
		}
	}

	Debug(dLog, "S%d success AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s], %d entries to append", rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role, len(arg.Entries))
	reply.Success = true
	var i int
	for i = 0; i < len(arg.Entries); i++ {
		if arg.PrevLogIndex+1+i > rf.lastIncludedIndex+len(rf.log) {
			break
		}
		if arg.Entries[i].Term == rf.log[arg.PrevLogIndex+i+1-rf.lastIncludedIndex-1].Term {
			continue
		}
		rf.log = rf.log[:arg.PrevLogIndex+1+i-rf.lastIncludedIndex-1]
		rf.persist()
		break
	}
	// append the other entries
	for j := i; j < len(arg.Entries); j++ {
		rf.log = append(rf.log, arg.Entries[j])
	}
	rf.persist()

	if arg.LeaderCommit > rf.cimmitIndex {
		newEntryIndex := rf.lastIncludedIndex + len(rf.log)
		if arg.LeaderCommit >= newEntryIndex {
			rf.cimmitIndex = newEntryIndex
		} else {
			rf.cimmitIndex = arg.LeaderCommit
		}
		Debug(dCommit, "S%d commit index %d", rf.me, rf.cimmitIndex)
		rf.sendApplyMsg()
	}

}

// appendEntries rpc handler
// resets the election timeouts so
// the other servers donot step forward as leaders when has already been elected
// func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, replay *AppendEntriesReplay) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	// defer rf.persist()
// 	Debug(dLog, "S%d Handle AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s]",
// 		rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role)
// 	defer func() {
// 		Debug(dLog, "S%d Return AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s]",
// 			rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role)
// 	}()

// 	replay.Term = rf.currentTerm
// 	replay.Success = false

// 	// replay false if log does not contain an entry
// 	// at prevLogIndex whose term matches prevLogTerm
// 	// 请求的任期小于当前执行的任期，需要拒绝这次请求

// 	if arg.Term < rf.currentTerm {
// 		Debug(dLog, "S%d Reject AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s]", rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role)
// 		return
// 	}

// 	// 接收到leader的appendenties reset timer
// 	rf.lastActiveTime = time.Now()

// 	// if received frim new leader: convert to follower
// 	if arg.Term > rf.currentTerm || rf.role == ROLE_CANDIDATE {
// 		Debug(dClient, "S%d convert to follower, LeaderId%d Term%d CurrentTerm%d role=[%s]", rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role)
// 		// 进入新的一轮，重置一些数据
// 		rf.currentTerm = arg.Term
// 		rf.role = ROLE_FOLLOW
// 		rf.votedFor = -1
// 		rf.leaderIndex = -1
// 		rf.persist()
// 	}

// 	rf.leaderIndex = arg.LeaderId

// 	if arg.PrevLogIndex < rf.lastIncludedIndex {
// 		replay.Success = false
// 		return
// 	}

// 	// 1.reply false if log doesnot contain an entry
// 	// at prevLogIndex whose term matches prevLogTerm
// 	myLastLogIndex := rf.lastIncludedIndex + len(rf.log)
// 	Debug(dLog, "S%d lastIncludedIndex=%d, argPrevLogIndex=%d, len(rf.log)=%d", rf.me, rf.lastIncludedIndex, arg.PrevLogIndex, len(rf.log))
// 	if arg.PrevLogIndex > 0 && (arg.PrevLogIndex > myLastLogIndex || (arg.PrevLogIndex > rf.lastIncludedIndex && rf.log[arg.PrevLogIndex-rf.lastIncludedIndex-1].Term != arg.PrevLogTerm)) {

// 		Debug(dError, "S%d has more log than S%d or term misMatches ", rf.me, arg.LeaderId)
// 		if arg.PrevLogIndex >= myLastLogIndex {
// 			Debug(dError, "S%d lastLogIndex=%d, while arg.PrevLogIndex=%d", rf.me, myLastLogIndex, arg.PrevLogIndex)
// 		}
// 		replay.Term, replay.Success = rf.currentTerm, false
// 		replay.ConflictLen = len(rf.log) // TODO check here maybe wrong shorter will not cause bugs
// 		if arg.PrevLogIndex >= 0 && arg.PrevLogIndex <= myLastLogIndex {
// 			Debug(dError, "S%d has more log than S%d / term misMatches ", rf.me, arg.LeaderId)
// 			replay.ConflictTerm = rf.log[arg.PrevLogIndex-rf.lastIncludedIndex-1].Term

// 			// find the first log index with the conflict term
// 			for i := arg.PrevLogIndex; i > rf.lastIncludedIndex; i-- {
// 				if rf.log[i-rf.lastIncludedIndex-1].Term == replay.ConflictTerm {
// 					replay.ConflictIndex = i
// 				}
// 			}
// 		}
// 		return
// 	}

// 	// 2 if an existing entry conflicts with a new
// 	// one(same index but different terms) delete the existing entry and all that follow it
// 	// Debug(dLog, "S%d AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s], %d entries to append", rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role, len(arg.Entries))
// 	Debug(dLog, "S%d lastIncludedIndex=%d, argPrevLogIndex=%d, len(rf.log)=%d", rf.me, rf.lastIncludedIndex, arg.PrevLogIndex, len(rf.log))
// 	var i int
// 	for i = 0; i < len(arg.Entries); i++ {
// 		if arg.PrevLogIndex+1+i > myLastLogIndex {
// 			break
// 		}
// 		if arg.Entries[i].Term == rf.log[arg.PrevLogIndex+i+1-rf.lastIncludedIndex-1].Term {
// 			continue
// 		}

// 		// if an exist entry conflicts with a new one
// 		// (same index but different terms)
// 		// delete the existing entry and all that follow it
// 		Debug(dLog, "S%d delete log from %d to %d cause confict with new one", rf.me, arg.PrevLogIndex+1+i, myLastLogIndex)
// 		rf.log = rf.log[:arg.PrevLogIndex+1+i-rf.lastIncludedIndex-1]
// 		rf.persist()
// 		break
// 	}
// 	// append the entry not in the log
// 	for j := i; j < len(arg.Entries); j++ {
// 		rf.log = append(rf.log, arg.Entries[j])
// 	}
// 	rf.persist()

// 	if arg.LeaderCommit > rf.cimmitIndex {
// 		newEntryIndex := myLastLogIndex
// 		if arg.LeaderCommit >= newEntryIndex {
// 			rf.cimmitIndex = newEntryIndex
// 		} else {
// 			rf.cimmitIndex = arg.LeaderCommit
// 		}
// 		Debug(dCommit, "S%d commit index %d", rf.me, rf.cimmitIndex)
// 		rf.sendApplyMsg()
// 	}
// 	replay.Term = rf.currentTerm
// 	replay.Success = true
// }

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, replay *AppendEntriesReplay) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, replay)
	return ok
}

func (rf *Raft) ticker_Leader() {
	timeouts := time.Duration(50) * time.Millisecond
	for !rf.killed() {
		rf.mu.Lock()

		if rf.role == ROLE_LEADER {
			// time.Sleep(timeouts)
			now := time.Now()
			if now.Sub(rf.lastBroadcastTime) >= timeouts {
				Debug(dTimer, "S%d Leader, checking heartbeats", rf.me)
				rf.mu.Unlock()
				rf.broadcastAppendEntries()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		// 状态检查
		if rf.role != ROLE_LEADER {
			rf.mu.Unlock()
			return
		}
		rf.lastBroadcastTime = time.Now()

		if rf.nextIdexs[i]-1 < rf.lastIncludedIndex {
			// the follower is behind the snapshot need to send snapshot
			Debug(dSnap, "S%d send snapshot to S%d because the nextId%d for S%d is behind rfLastIncludedLogIndex%d ", rf.me, i, rf.nextIdexs[i], i, rf.lastIncludedIndex)
			currentTerm := rf.currentTerm
			go rf.sendSnapshot(i, InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Snapshot:          rf.snapshot,
			}, currentTerm)
		} else {
			// else send appendEntries normally
			prevLogIndex := rf.nextIdexs[i] - 1
			prevLogTerm := -1

			if prevLogIndex > rf.lastIncludedIndex {
				prevLogTerm = rf.log[prevLogIndex-rf.lastIncludedIndex-1].Term
			} else if prevLogIndex == rf.lastIncludedIndex {
				prevLogTerm = rf.lastIncludedTerm
			}
			/*
				before lab2D code
			*/
			// if prevLogIndex >= 0 {
			// 	prevLogTerm = rf.log[prevLogIndex].Term
			// }

			var entries []LogEntry

			myLastLogIndex := rf.lastIncludedIndex + len(rf.log)
			if myLastLogIndex >= rf.nextIdexs[i] {
				Debug(dLog, "S%d length of log=%d, next index of S%d: %d", rf.me, len(rf.log), i, rf.nextIdexs[i])
				entries = rf.log[rf.nextIdexs[i]-rf.lastIncludedIndex-1:]
			}

			/*
				before lab2D code
			*/
			// if len(rf.log)-1 >= rf.nextIdexs[i] {
			// 	entries = rf.log[rf.nextIdexs[i]:]
			// 	Debug(dLog, "S%d length of log: %d, next index of S%d: %d", rf.me, len(rf.log), i, rf.nextIdexs[i])
			// }

			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rf.cimmitIndex,
			}

			go func(id int, args1 *AppendEntriesArgs, term int) {
				reply := AppendEntriesReplay{}
				Debug(dLeader, "S%d send AppendEntries to S%d", rf.me, id)
				if ok := rf.sendAppendEntries(id, args1, &reply); ok {
					Debug(dLeader, "S%d got resp ok from S%d", rf.me, id)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Term > rf.currentTerm {
						rf.role = ROLE_FOLLOW
						rf.leaderIndex = -1
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
					}
					// 这里需要注意下term可能改变了（网络原因） 保证当前term 发送时候的term 和接收者的term 一致
					if reply.Term == rf.currentTerm && term == rf.currentTerm {
						if reply.Success {
							// 更新成功 更新nextLogIndex 和 lastApplied commitIndex
							rf.nextIdexs[id] = prevLogIndex + len(entries) + 1
							rf.matchIndexs[id] = prevLogIndex + len(entries)

							// if there exists an n sunch that N > commitIndex,
							// a majority of matchIndex[i] >= N and log[N].term == currentTerm:
							// set commitIndex = N
							matches := make([]int, len(rf.peers))
							copy(matches, rf.matchIndexs)
							sort.Ints(matches)
							majority := (len(rf.peers) - 1) / 2                             // 排序后的中间 就是大多数
							for i := majority; i >= 0 && matches[i] > rf.cimmitIndex; i-- { //TODO take care
								if matches[i] == rf.lastIncludedIndex && rf.lastIncludedTerm == rf.currentTerm {
									rf.cimmitIndex = matches[i]
									Debug(dCommit, "S%d commit index %d", rf.me, rf.cimmitIndex)
									rf.sendApplyMsg()
									break
								} else if matches[i] > rf.lastIncludedIndex && rf.log[matches[i]-rf.lastIncludedIndex-1].Term == rf.currentTerm {
									rf.cimmitIndex = matches[i]
									Debug(dCommit, "S%d commit index %d", rf.me, rf.cimmitIndex)
									rf.sendApplyMsg()
									break
								}
							}
						} else {
							Debug(dLeader, "S%d get false from S%d", rf.me, id)
							// 更新失败，找到冲突的index 和term
							// TODO take care
							tmp := rf.nextIdexs[id]
							if reply.ConflictIndex == -1 {
								// replay to outdated request
								Debug(dError, "S%d next index %d for S%d is outdated", rf.me, rf.nextIdexs[id], id)
								return
							}
							if reply.ConflictTerm == -1 {
								// followers log shorter than rf.next[id]
								Debug(dError, "S%d next index %d for S%d is too long", rf.me, rf.nextIdexs[id], id)
								rf.nextIdexs[id] = reply.ConflictIndex
							} else if reply.ConflictTerm < args1.PrevLogTerm {
								Debug(dError, "S%d argPrevLogTerm %d mis match with S%d ConglictTerm %d, ConflictIndex %d ", rf.me, args1.PrevLogTerm, id, reply.ConflictTerm, reply.ConflictIndex)
								// go back to last term of the leader
								for rf.nextIdexs[id] > rf.lastIncludedIndex+1 && rf.log[rf.nextIdexs[id]-1-rf.lastIncludedIndex-1].Term == args1.PrevLogTerm {
									rf.nextIdexs[id]--
								}
								if rf.nextIdexs[id] != 1 && rf.nextIdexs[id] == rf.lastIncludedIndex+1 && rf.lastIncludedTerm == args1.PrevLogTerm {
									// we are hitting the snapshot
									rf.nextIdexs[id]--
								}
							} else {
								// reply.ConflictTerm > args1.PrevLogIndex
								// go back to last term of the follower
								rf.nextIdexs[id] = reply.ConflictIndex
							}
							Debug(dLog, "S%d nextIndex[%d] %d -> %d", rf.me, id, tmp, rf.nextIdexs[id])
							// rf.nextIdexs[id] = prevLogIndex
							// if rf.nextIdexs[id]-1 >= reply.ConflictLen {
							// 	//case 3 the follower's log is too short
							// 	rf.nextIdexs[id] = reply.ConflictLen
							// } else {
							// 	has := false
							// 	for i := rf.nextIdexs[id]; i >= reply.ConflictIndex; i-- {
							// 		// case 2 leader has conflict term in its log, nextIndex = leader's last entry for ConflictTerm
							// 		if i > rf.lastIncludedIndex {
							// 			if rf.log[i-rf.lastIncludedIndex-1].Term != reply.ConflictTerm {
							// 				has = true
							// 				rf.nextIdexs[id] -= 1
							// 			} else {
							// 				break
							// 			}
							// 		}
							// 	}

							// 	// case 1 leader does not have conflict term in its log nextIndex = conflictIndex
							// 	if !has {
							// 		rf.nextIdexs[id] = reply.ConflictIndex
							// 	}
							// }
						}
					}
					Debug(dLeader, "S%d appendEntries ends, peerTerm%d myCurrentTerm%d myRole[%s]", rf.me, reply.Term, rf.currentTerm, rf.role)

				}
			}(i, &args, rf.currentTerm)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendApplyMsg() {
	rf.moreApply = true
	rf.applyCond.Broadcast()
}

func (rf *Raft) appMsgApplier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for !rf.killed() {
		// rf.mu.Lock()
		// defer rf.mu.Unlock()
		if rf.cimmitIndex > rf.lastApplied {
			rf.lastApplied++
			Debug(dCommit, "S%d start apply index %d, lastIncludeIndex %d current log length is %d", rf.me, rf.lastApplied, rf.lastIncludedIndex, len(rf.log))
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Command,
				CommandIndex: rf.lastApplied,
				CommandTerm:  rf.log[rf.lastApplied-rf.lastIncludedIndex-1].Term,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}
func (rf *Raft) ticker_Vote() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == ROLE_FOLLOW {
			rf.mu.Unlock()
			rf.timeouts = time.Duration(150+rand.Int31n(190)) * time.Millisecond
			rf.mu.Lock()
			now := time.Now()
			if now.Sub(rf.lastActiveTime) > rf.timeouts {
				rf.role = ROLE_CANDIDATE
				Debug(dTimer, "S%d Follower -> Candidate at time : %d", rf.me, time.Now().Unix())
				go rf.startElection()
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// // 此处会产生大量的rpc 导致user time 增加
// // 也就是会一直发送broadcastVote
func (rf *Raft) startElection() {
	for !rf.killed() {
		rf.mu.Lock()
		now := time.Now()
		if rf.role == ROLE_CANDIDATE {
			if now.Sub(rf.lastActiveTime) > rf.timeouts {
				Debug(dClient, "S%d election timeouts restart a new election now - lastAct = %d ms", rf.me, now.Sub(rf.lastActiveTime).Milliseconds())
				// 重新设置超时时间

				rf.timeouts = time.Duration(150+rand.Int31n(190)) * time.Millisecond
				//通过Debug 打印新的超时时间
				Debug(dInfo, "S%d new timeouts %d ms", rf.me, rf.timeouts.Milliseconds())
				// 开启新的选举
				rf.currentTerm++
				rf.role = ROLE_CANDIDATE
				rf.votedFor = rf.me
				rf.lastActiveTime = time.Now()
				rf.persist()
				rf.mu.Unlock()
				go rf.broadcastVote()
			} else {
				rf.mu.Unlock()
			}
		} else {
			rf.mu.Unlock()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) broadcastVote() {
	voteGranted := 1
	voteCount := 1
	cond := sync.NewCond(&rf.mu)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		Debug(dVote, "S%d send request to S%d for vote", rf.me, i)
		if rf.role != ROLE_CANDIDATE {
			rf.mu.Unlock()
			return
		}
		lastLogIndex := rf.lastIncludedIndex + len(rf.log)
		// lastLogIndex := len(rf.log) - 1
		lastLogTerm := -1
		if lastLogIndex > rf.lastIncludedIndex {
			lastLogTerm = rf.log[lastLogIndex-rf.lastIncludedIndex-1].Term
		}
		if lastLogIndex == rf.lastIncludedIndex {
			lastLogTerm = rf.lastIncludedTerm
		}

		args := RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		rf.mu.Unlock()

		go func(peer int, args *RequestVoteArgs, term int) {
			resp := RequestVoteReply{}
			if ok := rf.sendRequestVote(peer, args, &resp); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				voteCount++
				// 过期检查
				if resp.Term > rf.currentTerm {
					rf.role = ROLE_FOLLOW
					rf.currentTerm = resp.Term
					rf.votedFor = -1
					rf.leaderIndex = -1
					rf.persist()
					return
				}
				// 状态检查
				if rf.role != ROLE_CANDIDATE {
					return
				}
				if resp.VoteGranted && term == rf.currentTerm { //保证是同一轮的投票
					voteGranted++
				}
				cond.Broadcast()
			} else {
				cond.Broadcast()
				return
			}
		}(i, &args, rf.currentTerm)
	}

	// go func() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for voteGranted <= len(rf.peers)/2 && voteCount < len(rf.peers) && rf.role == ROLE_CANDIDATE {
		cond.Wait()
	}
	if voteGranted > len(rf.peers)/2 && rf.role == ROLE_CANDIDATE {
		rf.role = ROLE_LEADER
		rf.leaderIndex = rf.me
		rf.lastBroadcastTime = time.Unix(0, 0)
		now := time.Now()
		costsTime := now.Sub(rf.lastActiveTime).Milliseconds()
		Debug(dVote, "S%d got majority votes and becomes Leader Spend %d millseconds", rf.me, costsTime)

		// 升级为leader 初始化nextIndex 和matchIndex
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIdexs[i] = rf.lastIncludedIndex + len(rf.log) + 1
			Debug(dLog, "S%d nextIndex[%d] = %d", rf.me, i, rf.nextIdexs[i])
			// rf.nextIdexs[i] = len(rf.log) // initialized to leader last log index + 1
			rf.matchIndexs[i] = -1
		}
		return
	} else {
		Debug(dVote, "S%d did not got majortity votes election faild", rf.me)
		return
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.lastIncludedIndex + len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.role == ROLE_LEADER

	if !isLeader {
		Debug(dClient, "S%d is not leader return index %d to service", rf.me, index)
		return index, term, isLeader
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist()
	rf.matchIndexs[rf.me] = index
	rf.nextIdexs[rf.me] = index + 1
	Debug(dLeader, "S%d append command: index: %d, term: %d", rf.me, index, term)
	go rf.broadcastAppendEntries()
	return index, term, isLeader

	// index := -1
	// term := -1
	// isLeader := true

	// // Your code here (2B).
	// if rf.killed() {
	// 	return index, term, false
	// }

	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// // defer rf.persist()
	// index = rf.lastIncludedIndex + len(rf.log) + 1
	// term = rf.currentTerm
	// isLeader = rf.role == ROLE_LEADER

	// if isLeader {
	// 	log := LogEntry{
	// 		Term:    rf.currentTerm,
	// 		Command: command,
	// 	}
	// 	rf.log = append(rf.log, log)

	// 	index = rf.lastIncludedIndex + len(rf.log)
	// 	// index = len(rf.log) - 1
	// 	term = rf.currentTerm

	// 	rf.matchIndexs[rf.me] = index
	// 	rf.nextIdexs[rf.me] = index + 1

	// 	// rf.matchIndexs[rf.me] = len(rf.log) - 1
	// 	// rf.nextIdexs[rf.me] = len(rf.log)
	// 	Debug(dLeader, "S%d append command: index: %d, term: %d", rf.me, index, term)
	// 	rf.persist()
	// }
	// Debug(dClient, "S%d start lastIncludeLogIndex=%d", rf.me, rf.lastIncludedIndex)
	// return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// inilized from state persisted after a crash
	// rf.readPersist(persister.ReadRaftState())
	// rf.snapshot = persister.ReadSnapshot()

	// initialize volatile fields:
	// rf.cimmitIndex = 0
	// rf.lastApplied = rf.lastIncludedIndex

	// rf.role = ROLE_FOLLOW
	// rf.applyCh = applyCh
	// rf.applyCond = sync.NewCond(&rf.mu)
	// rf.moreApply = false

	rf.role = ROLE_FOLLOW
	rf.leaderIndex = -1
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh

	rf.currentTerm = 0

	rf.log = make([]LogEntry, 0)

	rf.nextIdexs = make([]int, len(peers))
	rf.matchIndexs = make([]int, len(peers))

	rf.moreApply = false
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	//initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.cimmitIndex = 0
	rf.lastApplied = rf.lastIncludedIndex

	go rf.ticker_Leader()
	go rf.ticker_Vote()
	go rf.appMsgApplier()
	// start ticker goroutine to start elections
	// go rf.ticker()
	Debug(dClient, "S%d start", me)
	return rf
}
