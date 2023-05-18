package shardctrler

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	stateMachine  ConfigStateMachine
	lastOperation map[int64]OperationContext
	notifyChans   map[int]chan *CommandReply
	configs       []Config // indexed by config num
}

// rpc handler for client requests.
func (sc *ShardCtrler) Command(args *CommandArgs, reply *CommandReply) {
	sc.mu.Lock()
	defer DPrintf("controller %d process requenst %v return response %v", sc.me, args, reply)
	// duplicate request return directily
	if args.Op != OpQuery && sc.isDuplicateRequest(args.ClientId, args.CommandId) {
		lastReponse := sc.lastOperation[args.ClientId].LastResponse
		reply.Err = lastReponse.Err
		reply.Config = lastReponse.Config
		sc.mu.Unlock()
		return
	}

	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	sc.mu.Lock()
	ch := sc.getNotifyChan(index)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.Config = result.Config
	case <-time.After(1000 * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go func() {
		sc.mu.Lock()
		delete(sc.notifyChans, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	operationContext, ok := sc.lastOperation[clientId]
	return ok && commandId <= operationContext.MaxAppliedCommandId
}

func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandReply {
	if _, ok := sc.notifyChans[index]; !ok {
		sc.notifyChans[index] = make(chan *CommandReply, 1)
	}
	return sc.notifyChans[index]
}

func (sc *ShardCtrler) removeOutdatedChans(index int) {
	delete(sc.notifyChans, index)
}

func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandReply {
	var config Config
	var err Err
	switch command.Op {
	case OpJoin:
		err = sc.stateMachine.Join(command.Servers)
	case OpLeave:
		err = sc.stateMachine.Leave(command.GIDs)
	case OpMove:
		err = sc.stateMachine.Move(command.Shard, command.GID)
	case OpQuery:
		config, err = sc.stateMachine.Query(command.Num)
	}
	return &CommandReply{Err: err, Config: config}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
}

func (sc *ShardCtrler) Killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for sc.Killed() == false {
		select {
		// DPrintf("{Node %v} tries to apply message %v", sc.rf.Me(), message)
		case msg := <-sc.applyCh:
			DPrintf("Controller %d try to apply message %v", sc.me, msg)
			if msg.CommandValid {
				var response *CommandReply
				command := msg.Command.(Command)
				sc.mu.Lock()

				if command.Op != OpQuery && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					lastResponse := sc.lastOperation[command.ClientId].LastResponse
					response.Config = lastResponse.Config
					response.Err = lastResponse.Err
					DPrintf("Controller%d doesn't apply duplicate request %v", sc.me, command)
					return
				} else {
					response = sc.applyLogToStateMachine(command)
					if command.Op != OpQuery {
						sc.lastOperation[command.ClientId] = OperationContext{MaxAppliedCommandId: command.CommandId, LastResponse: response}
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					ch := sc.getNotifyChan(msg.CommandIndex)
					ch <- response
				}
				sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", msg))
			}

		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg)
	sc := &ShardCtrler{
		me:            me,
		mu:            sync.Mutex{},
		applyCh:       applyCh,
		lastOperation: make(map[int64]OperationContext),
		dead:          0,
		notifyChans:   make(map[int]chan *CommandReply),
		configs:       make([]Config, 1),
		stateMachine:  NewMemoryStateConfigMachine(),
		rf:            raft.Make(servers, me, persister, applyCh),
	}
	// Your code here.
	go sc.applier()
	DPrintf("Controller %d start", me)
	return sc
}
