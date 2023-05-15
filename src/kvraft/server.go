package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"

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

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type    string
	Key     string
	Value   string
	Version int64
	ID      int
	Leader  int // only used for newleader command
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap           map[string]string         // map for storage
	pendingChannels map[int]chan handlerReply // map from log index to channel only valid when it is leader
	pendingMap      map[int]Op                // map from log index to cmd
	dupMap          map[int]int64             // map from clinet id to version

	persister *raft.Persister
}
type handlerReply struct {
	err   Err
	value string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	DPrintf("server %d: process get\n", kv.me)
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("Server %d GET: not leader\n", kv.me)
		return
	}
	DPrintf("server %d GET: is leader\n", kv.me)
	op := Op{Type: "Get", Key: args.Key, Version: -1, ID: -1, Leader: -1}
	index, _, _ := kv.rf.Start(op)
	DPrintf("server %d: get reply from raft index %d\n", kv.me, index)
	kv.pendingChannels[index] = make(chan handlerReply)
	kv.pendingMap[index] = op
	ch := kv.pendingChannels[index]
	kv.mu.Unlock()

	hreply := <-ch
	kv.mu.Lock()
	reply.Err = hreply.err
	reply.Value = hreply.value
	DPrintf("server %d: get %v Err %v\n", kv.me, reply.Value, reply.Err)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("server %d process PutAppend\n", kv.me)

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		DPrintf("Server %d PutAppend: not leader\n", kv.me)
		return
	}
	DPrintf("server %d PutAppend: is leader\n", kv.me)

	// duplicate detection
	_, ok := kv.dupMap[args.ID]
	if !ok {
		// new client
		// initilaize
		kv.dupMap[args.ID] = -1
	}
	DPrintf("Server %d PutAppend: duplicate detection\n", kv.me)
	if args.Version <= kv.dupMap[args.ID] {
		// already processed
		DPrintf("Server %d PutAppend: duplicate detected\n", kv.me)
		reply.Err = OK
		return
	}
	op := Op{
		Type:    args.Op,
		Key:     args.Key,
		Value:   args.Value,
		Version: args.Version,
		ID:      args.ID,
		Leader:  -1,
	}
	index, _, _ := kv.rf.Start(op)
	kv.pendingChannels[index] = make(chan handlerReply)
	kv.pendingMap[index] = op
	ch := kv.pendingChannels[index]
	kv.mu.Unlock()
	DPrintf("server %d waiting for reply\n", kv.me)
	hreply := <-ch
	kv.mu.Lock()
	reply.Err = hreply.err
	DPrintf("finish processing putappend: %v, Err %v\n", op.Value, reply.Err)

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applyListener() {
	for !kv.killed() {
		DPrintf("Server %d listensing", kv.me)
		DPrintf("current goroutine nmumber: %d", runtime.NumGoroutine())
		msg := <-kv.applyCh
		DPrintf("Server %d get reply", kv.me)
		kv.mu.Lock()
		if msg.CommandValid {
			DPrintf("Server %d get lock in applyListener\n", kv.me)
			cmd := msg.Command.(Op)
			DPrintf("Command content : %s, %s, %s\n", cmd.Type, cmd.Key, cmd.Value)
			if cmd.Type == "NewLeader" {
				if cmd.Leader == kv.me {
					kv.mu.Unlock()
					continue
				}
				for i, ch := range kv.pendingChannels {
					// we are not leader anymore
					DPrintf("Server %d not leader anymore\n", kv.me)
					ch <- handlerReply{err: ErrWrongLeader, value: ""}
					close(ch)
					delete(kv.pendingChannels, i)
					delete(kv.pendingMap, i)

				}
				kv.mu.Unlock()
				continue
			}

			if cmd.Type != "Get" {
				_, ok := kv.dupMap[cmd.ID]
				if !ok {
					// new client
					// initialize
					kv.dupMap[cmd.ID] = -1
				}
				if cmd.Version <= kv.dupMap[cmd.ID] {
					// duplicate detected
					goto sendHreply
				}
				// not a duplicate command
				kv.dupMap[cmd.ID] = cmd.Version

				switch cmd.Type {
				case "Append":
					kv.kvMap[cmd.Key] = kv.kvMap[cmd.Key] + cmd.Value
					// DPrintf("Server %d append %s now kvMap:%v\n", kv.me, cmd.Value, kv.kvMap)
				case "Put":
					kv.kvMap[cmd.Key] = cmd.Value
					// DPrintf("Server %d put %s now kvMap:%v\n", kv.me, cmd.Value, kv.kvMap)

				default:
					panic(fmt.Sprintf("server %d: invalid command type %s\n", kv.me, cmd.Type))
				}

			}
		sendHreply:
			_, hasKey := kv.pendingChannels[msg.CommandIndex]
			oldCmd := kv.pendingMap[msg.CommandIndex]

			if hasKey {
				if cmd != oldCmd {
					for i, ch := range kv.pendingChannels {
						DPrintf("Server %d send in loop\n", kv.me)
						ch <- handlerReply{err: ErrWrongLeader, value: ""}
						close(ch)
						delete(kv.pendingChannels, i)
						delete(kv.pendingMap, i)
					}
				} else {
					DPrintf("Server %d send in else\n", kv.me)
					var hreply handlerReply
					if cmd.Type == "Get" {
						elem, ok := kv.kvMap[cmd.Key]
						// DPrintf("Server %d get %s\n", kv.me, elem)
						if ok {
							hreply.err = OK
							hreply.value = elem
						} else {
							hreply.err = ErrNoKey
							hreply.value = ""
						}
					} else {
						hreply.err = OK
						hreply.value = ""
					}
					kv.pendingChannels[msg.CommandIndex] <- hreply
					close(kv.pendingChannels[msg.CommandIndex])
					delete(kv.pendingChannels, msg.CommandIndex)
					delete(kv.pendingMap, msg.CommandIndex)
				}
				DPrintf("Server %d sending finish\n", kv.me)
			} else {
				for i, ch := range kv.pendingChannels {
					// clear all
					DPrintf("Server %d send in loop1\n", kv.me)
					ch <- handlerReply{err: ErrWrongLeader, value: ""}
					close(ch)
					delete(kv.pendingChannels, i)
					delete(kv.pendingMap, i)
				}
			}

			// check whether need to snapshot
			// if kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate)*0.95 {
			// 	w := new(bytes.Buffer)
			// 	e := labgob.NewEncoder(w)
			// 	e.Encode(kv.dupMap)
			// 	e.Encode(kv.kvMap)
			// 	data := w.Bytes()
			// 	kv.rf.Snapshot(msg.CommandIndex, data)
			// }
		} else if msg.SnapshotValid {
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.readPersist(msg.Snapshot)
			}
		} else {
			newCmd := Op{
				Type:    "NewLeader",
				Key:     "NewLeader_invalid",
				Value:   "NewLeader_invalid",
				Version: -1,
				ID:      -1,
				Leader:  -1,
			}
			kv.rf.Start(newCmd)
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var dupMap map[int]int64 // map used to detecte duplicate msg
	var kvMap map[string]string
	if d.Decode(&dupMap) != nil || d.Decode(&kvMap) != nil {
		fmt.Printf("sever %d readPersist(kv): decode error", kv.me)
	} else {
		kv.dupMap = dupMap
		kv.kvMap = kvMap
	}
}

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
	kv.kvMap = make(map[string]string)
	kv.pendingChannels = make(map[int]chan handlerReply)
	kv.pendingMap = make(map[int]Op)
	kv.dupMap = make(map[int]int64)

	kv.persister = persister
	kv.readPersist(persister.ReadSnapshot())
	// kv.readSnapshot(kv.persister.ReadSnapshot())
	go kv.applyListener()
	return kv
}
