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
	startIndex  int

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
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.me == rf.leaderIndex)
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
	index := len(rf.log) - 1
	// start index
	for key := range rf.log {
		if key < index {
			index = key
		}
	}
	rf.startIndex = index
	e.Encode(rf.startIndex)
	data := w.Bytes()
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
	d.Decode(&rf.startIndex)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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
	// 1. save snapshot
	// 2. update lastIncludedIndex and lastIncludedTerm
	// 3. update log
	// 4. update persister

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[index-rf.startIndex].Term
	rf.log = rf.log[index-rf.startIndex+1:]
	rf.startIndex = index
	rf.persist()

	// send snapshot to all peers
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendInstallSnapshot(i, snapshot)
		}
	}
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapshot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}

}

type InstallSnapshotArgs struct {
	Term              int    //leader's term
	LeaderId          int    //
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastincludedIndex
	Snapshot          []byte // raw bytes of the snapshot chunk starting
	// Done              bool   // true if theis is the last chunk
}

type InstallSnapshotReplyArgs struct {
	Term int //currentTerm for leader tot update itself
}

func (rf *Raft) InstallSnapshot(arg *InstallSnapshotArgs, reply *InstallSnapshotReplyArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// reply immediately if term < currentTerm
	if arg.Term < rf.currentTerm {
		return
	}
	// discard any existing or partial snapshot with a smaller index
	if arg.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	}
	// if existing log entry has same index and term as snapshot's last included entry,
	// retain log entries following it and reply
	if len(rf.log) > 0 && arg.LastIncludedIndex == rf.lastIncludedIndex && arg.LastIncludedTerm == rf.lastIncludedTerm {
		rf.log = rf.log[rf.lastIncludedIndex:]
	} else {
		// discard the entire log
		rf.log = make([]LogEntry, 0)
	}
	// reset state machine using snapshot contents
	rf.lastIncludedIndex = arg.LastIncludedIndex
	rf.lastIncludedTerm = arg.LastIncludedTerm
	rf.persister.SaveStateAndSnapshot(rf.encodeRaftState(), arg.Snapshot)
	rf.persist()
	// send snapshot to applyCh
	applyMsg := ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: arg.LastIncludedIndex,

		SnapshotValid: true,
		Snapshot:      arg.Snapshot,
		SnapshotTerm:  arg.LastIncludedTerm,
		SnapshotIndex: arg.LastIncludedIndex,
	}
	rf.applyCh <- applyMsg

}

func (rf *Raft) sendInstallSnapshot(server int, snapshot []byte) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Snapshot:          snapshot,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReplyArgs{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.role = ROLE_FOLLOW
			rf.persist()
		}
	}
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	return data
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
	defer rf.persist()

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
	}

	// 还需要判断log是否是最新的
	// 1. 当前node 的log index = 0
	// 2. 请求的最后一个index的term 大于当前node 的最后一个index 的term
	// 3.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (len(rf.log) == 0 || (args.LastLogTerm > rf.log[len(rf.log)-1].Term) || (args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1)) {
		rf.votedFor = args.CandidateId
		// rf.persist()
		// 成功投票，刷新自己的心跳时间
		rf.lastActiveTime = time.Now()
		reply.VoteGranted = true
		// rf.role = ROLE_FOLLOW
		Debug(dClient, "S%d voted to S%d", rf.me, args.CandidateId)
		return
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

// appendEntries rpc handler
// resets the election timeouts so
// the other servers donot step forward as leaders when has already been elected
func (rf *Raft) AppendEntries(arg *AppendEntriesArgs, replay *AppendEntriesReplay) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	Debug(dLog, "S%d Handle AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s]",
		rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role)
	defer func() {
		Debug(dLog, "S%d Return AppendEntries, LeaderId%d Term%d CurrentTerm%d role=[%s]",
			rf.me, arg.LeaderId, arg.Term, rf.currentTerm, rf.role)
	}()

	replay.Term = rf.currentTerm
	replay.Success = false

	// TODO  replay false if log does not contain an entry
	// at prevLogIndex whose term matches prevLogTerm
	// 请求的任期小于当前执行的任期，需要拒绝这次请求

	if arg.Term < rf.currentTerm {
		return
	}

	// 接收到leader的appendenties reset timer
	rf.lastActiveTime = time.Now()

	// if received frim new leader: convert to follower
	if arg.Term > rf.currentTerm || rf.role == ROLE_CANDIDATE {
		// 进入新的一轮，重置一些数据
		rf.currentTerm = arg.Term
		rf.role = ROLE_FOLLOW
		rf.votedFor = -1
		rf.leaderIndex = -1
	}

	rf.leaderIndex = arg.LeaderId
	// rf.persist()

	// TODO

	// 1.reply false if log doesnot contain an entry
	// at prevLogIndex whose term matches prevLogTerm
	if arg.PrevLogIndex >= len(rf.log) || (arg.PrevLogIndex >= 0 && rf.log[arg.PrevLogIndex].Term != arg.PrevLogTerm) {
		replay.Term, replay.Success = rf.currentTerm, false
		replay.ConflictLen = len(rf.log)
		if arg.PrevLogIndex >= 0 && arg.PrevLogIndex < len(rf.log) {
			// DPrintf("S%d has more log than S%d / term misMatches ", rf.me, arg.LeaderId)
			Debug(dError, "S%d has more log than S%d / term misMatches ", rf.me, arg.LeaderId)

			replay.ConflictTerm = rf.log[arg.PrevLogIndex].Term
			for i := arg.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term == replay.ConflictTerm {
					replay.ConflictIndex = i
				} else {
					break
				}
			}
		}
		return
	}
	// 2 if an existing entry conflicts with a new
	// one(same index but different terms) delete the existing entry
	// and all that follow it
	misMatchIndex := -1
	for i := range arg.Entries {
		if arg.PrevLogIndex+1+i >= len(rf.log) || rf.log[arg.PrevLogIndex+1+i].Term != arg.Entries[i].Term {
			misMatchIndex = i
			break
		}
	}
	// 3 append any new entries not already in the log
	if misMatchIndex != -1 {
		rf.log = append(rf.log[:arg.PrevLogIndex+1+misMatchIndex], arg.Entries[misMatchIndex:]...)
	}
	// 4 if leaderCommit > commitIndex set commitIndex = min(leaderCommit, index of last new entry)
	if arg.LeaderCommit > rf.cimmitIndex {
		newEntryIndex := len(rf.log) - 1
		if arg.LeaderCommit >= newEntryIndex {
			rf.cimmitIndex = newEntryIndex
		} else {
			rf.cimmitIndex = arg.LeaderCommit
		}

		Debug(dCommit, "S%d commit index %d", rf.me, rf.cimmitIndex)

		rf.sendApplyMsg()
	}
	replay.Term = rf.currentTerm
	replay.Success = true
	return
}

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

		prevLogIndex := rf.nextIdexs[i] - 1
		prevLogTerm := -1
		if prevLogIndex >= 0 {
			prevLogTerm = rf.log[prevLogIndex].Term
		}

		var entries []LogEntry
		if len(rf.log)-1 >= rf.nextIdexs[i] {
			entries = rf.log[rf.nextIdexs[i]:]
			Debug(dLog, "S%d length of log: %d, next index of S%d: %d", rf.me, len(rf.log), i, rf.nextIdexs[i])

		}

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
			if ok := rf.sendAppendEntries(id, args1, &reply); ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.role = ROLE_FOLLOW
					rf.leaderIndex = -1
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					// rf.persist()
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
						majority := (len(rf.peers) - 1) / 2 // 排序后的中间 就是大多数
						for i := majority; i >= 0 && matches[i] > rf.cimmitIndex; i-- {
							if rf.log[matches[i]].Term == rf.currentTerm {
								rf.cimmitIndex = matches[i]
								Debug(dCommit, "S%d commit index %d", rf.me, rf.cimmitIndex)

								rf.sendApplyMsg()
								break
							}
						}
					} else {
						// 更新失败，找到冲突的index 和term
						rf.nextIdexs[id] = prevLogIndex
						if rf.nextIdexs[id]-1 >= reply.ConflictLen {
							//case 3 the follower's log is too short
							rf.nextIdexs[id] = reply.ConflictLen
						} else {
							has := false
							for i := rf.nextIdexs[id] - 1; i >= reply.ConflictIndex; i-- {
								// case 2 leader has conflict term in its log, nextIndex = leader's last entry for ConflictTerm
								if rf.log[i].Term != reply.ConflictTerm {
									has = true
									rf.nextIdexs[id] -= 1
								} else {
									break
								}
							}

							// case 1 leader does not have conflict term in its log nextIndex = conflictIndex
							if !has {
								rf.nextIdexs[id] = reply.ConflictIndex
							}

							// for i := rf.nextIdexs[id] - 1; i >= reply.ConflictIndex; i-- {
							// 	if rf.log[i].Term != reply.ConflictTerm {
							// 		rf.nextIdexs[id] -= 1
							// 	} else {
							// 		break
							// 	}
							// }
						}
					}
				}
				Debug(dLeader, "S%d appendEntries ends, peerTerm%d myCurrentTerm%d myRole[%s]", rf.me, reply.Term, rf.currentTerm, rf.role)

			}
		}(i, &args, rf.currentTerm)
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendApplyMsg() {
	rf.moreApply = true
	rf.applyCond.Broadcast()
}

func (rf *Raft) appMsgApplier() {
	for !rf.killed() {
		rf.mu.Lock()

		for !rf.moreApply {
			rf.applyCond.Wait()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
		commitIndex := rf.cimmitIndex
		lastApplied := rf.lastApplied
		entries := rf.log
		rf.moreApply = false
		rf.mu.Unlock()
		for i := lastApplied + 1; i <= commitIndex; i++ {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      entries[i].Command,
				CommandIndex: i + 1,
			}
			Debug(dCommit, "S%d apply index %d - 1", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = i
			rf.mu.Unlock()
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
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := -1
		if lastLogIndex >= 0 {
			lastLogTerm = rf.log[lastLogIndex].Term
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
				defer rf.persist()
				voteCount++
				// 过期检查
				if resp.Term > rf.currentTerm {
					rf.role = ROLE_FOLLOW
					rf.currentTerm = resp.Term
					rf.votedFor = -1
					rf.leaderIndex = -1
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
			rf.nextIdexs[i] = len(rf.log) // initialized to leader last log index + 1
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	isLeader = rf.role == ROLE_LEADER
	if isLeader {
		log := LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, log)
		index = len(rf.log) - 1
		term = rf.currentTerm

		rf.matchIndexs[rf.me] = len(rf.log) - 1
		rf.nextIdexs[rf.me] = len(rf.log)
		Debug(dLeader, "S%d start received command: index: %d, term: %d", rf.me, index, term)

	}
	return index + 1, term, isLeader
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		time.Sleep(10 * time.Millisecond)

	}
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
	rf.role = ROLE_FOLLOW
	rf.leaderIndex = -1
	rf.votedFor = -1
	rf.lastActiveTime = time.Now()
	rf.applyCh = applyCh

	rf.currentTerm = 0

	rf.log = make([]LogEntry, 0)
	rf.cimmitIndex = -1
	rf.lastApplied = -1

	rf.nextIdexs = make([]int, len(peers))
	rf.matchIndexs = make([]int, len(peers))

	rf.moreApply = false
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.ticker_Leader()
	go rf.ticker_Vote()
	go rf.appMsgApplier()
	// start ticker goroutine to start elections
	// go rf.ticker()
	Debug(dClient, "S%d start", me)
	return rf
}
