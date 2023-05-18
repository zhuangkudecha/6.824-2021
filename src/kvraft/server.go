package kvraft

import (
	"bytes"
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
	// kvMap map[string]string // map for storage
	// pendingChannels map[int]chan handlerReply // map from log index to channel only valid when it is leader
	// pendingMap      map[int]Op                // map from log index to cmd
	// dupMap          map[int]int64             // map from clinet id to version

	persister           *raft.Persister
	db                  *KvDb
	LastClientOperation map[int64]ClientOperation
	notifyChans         map[int]chan ChanResult
	lastApplied         int
}

type ClientOperation struct {
	SequenceNum int
	Reponse     string
	Err         Err
}

type ChanResult struct {
	response string
	Err      Err
}

type PersistData struct {
	Db                  *KvDb
	LastClientOperation map[int64]ClientOperation
}

func (kv *KVServer) HandleCommand(request *ClientRequestArgs, reply *ClientRequestReply) {
	kv.mu.Lock()
	if kv.isDuplicateRequestWithoutLock(request.ClientId, request.SequenceNum) {
		reply.Status = true
		clientOperation := kv.LastClientOperation[request.ClientId]
		reply.Response = clientOperation.Reponse
		reply.Err = clientOperation.Err
		DPrintf("Server %v: %v-%v is duplicate request, return directly", kv.me, request.ClientId, request.SequenceNum)
		kv.mu.Unlock()
		return
	}

	if kv.isOutdateRequestWithoutLock(request.ClientId, request.SequenceNum) {
		reply.Status = true
		reply.Response = ""
		reply.Err = ErrNone
		DPrintf("Server %v: %v-%v is outdate request, return response ", kv.me, request.ClientId, request.SequenceNum)
		kv.mu.Unlock()
		return
	}

	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(*request)
	if !isLeader {
		reply.Status = false
		reply.Response = ""
		reply.Err = ErrWrongLeader
		return
	}
	notify := kv.createNotifyChan(index)
	DPrintf("Server %v: %v-%v is leader, start to wait for notify", kv.me, request.ClientId, request.SequenceNum)
	select {
	case res := <-notify:
		reply.Status = true
		reply.Response = res.response
		reply.Err = res.Err
		DPrintf("Server %v: %v-%v get notify, return ", kv.me, request.ClientId, request.SequenceNum)
	case <-time.After(ExecuteTimeout):
		reply.Status = false
		reply.Response = ""
		reply.Err = ErrTimeout
		DPrintf("Server %v: %v-%v timeout", kv.me, request.ClientId, request.SequenceNum)
	}

	go func() {
		// release notify chan
		kv.mu.Lock()
		close(kv.notifyChans[index])
		delete(kv.notifyChans, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) isDuplicateRequestWithoutLock(ClinetId int64, SequenceNum int) bool {
	if value, ok := kv.LastClientOperation[ClinetId]; ok {
		if value.SequenceNum == SequenceNum {
			return true
		}
	}
	return false
}

func (kv *KVServer) isOutdateRequestWithoutLock(ClinetId int64, SequenceNum int) bool {
	if value, ok := kv.LastClientOperation[ClinetId]; ok {
		if value.SequenceNum > SequenceNum {
			return true
		}
	}
	return false
}

func (kv *KVServer) createNotifyChan(index int) chan ChanResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res := make(chan ChanResult)
	kv.notifyChans[index] = res
	return res
}

func (kv *KVServer) saveNotifyChanWithoutLock(index int, res ChanResult) {
	ch, ok := kv.notifyChans[index]
	if !ok {
		return
	}
	ch <- res
}

func (kv *KVServer) saveClientRequestReplyWithoutLock(clientId int64, sequenceNum int, reponse string, err Err) {
	kv.LastClientOperation[clientId] = ClientOperation{
		SequenceNum: sequenceNum,
		Reponse:     reponse,
		Err:         err,
	}
}

func (kv *KVServer) needSnapshotWithoutLock() bool {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
		return true
	} else {
		return false
	}
}

func (kv *KVServer) takeSnapshotWithoutLock(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	persistData := PersistData{
		Db:                  kv.db,
		LastClientOperation: kv.LastClientOperation,
	}
	e.Encode(persistData)
	data := w.Bytes()
	kv.rf.Snapshot(index, data)
}

func (kv *KVServer) restoreFromSnapshotWithoutLock(data []byte) {
	if data == nil || len(data) == 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	persistData := &PersistData{}
	if d.Decode(&persistData) == nil {
		DPrintf("Server %v Read existed persist data: %+v", kv.me, persistData)
		kv.db = persistData.Db
		kv.LastClientOperation = persistData.LastClientOperation
	}
}

func (kv *KVServer) executeDbCommandWithoutLock(command Command) (string, Err) {
	if command.Op == OpRead {
		return kv.db.Get(command.Key)
	} else if command.Op == OpPut {
		err := kv.db.Put(command.Key, command.Value)
		return "", err
	} else if command.Op == OpAppend {
		err := kv.db.Append(command.Key, command.Value)
		return "", err
	} else {
		return "", ErrNone
	}

}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			if msg.CommandValid {
				chanResult := ChanResult{}
				if msg.CommandIndex > kv.lastApplied {
					args := msg.Command.(ClientRequestArgs)
					// check if dup request
					if kv.isDuplicateRequestWithoutLock(args.ClientId, args.SequenceNum) {
						clientOperation := kv.LastClientOperation[args.ClientId]
						chanResult.response = clientOperation.Reponse
						chanResult.Err = clientOperation.Err
						DPrintf("Server %v: %v-%v is duplicate request", kv.me, args.ClientId, args.SequenceNum)
					} else {
						response, err := kv.executeDbCommandWithoutLock(args.Command)
						chanResult.response = response
						chanResult.Err = err
						DPrintf("Server %v: %v-%v execute command %v", kv.me, args.ClientId, args.SequenceNum, args.Command)
						kv.saveClientRequestReplyWithoutLock(args.ClientId, args.SequenceNum, response, err)
					}

					if kv.needSnapshotWithoutLock() {
						kv.takeSnapshotWithoutLock(msg.CommandIndex)
					}
				}
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == msg.CommandTerm {
					kv.saveNotifyChanWithoutLock(msg.CommandIndex, chanResult)
				}
			} else if msg.SnapshotValid {
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.restoreFromSnapshotWithoutLock(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
			}
			kv.mu.Unlock()
		case <-time.After(10 * time.Second):
		}
	}
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
	labgob.Register(ClientRequestArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// kv.kvMap = make(map[string]string)
	// kv.pendingChannels = make(map[int]chan handlerReply)
	// kv.pendingMap = make(map[int]Op)
	// kv.dupMap = make(map[int]int64)

	kv.persister = persister
	kv.lastApplied = 0
	kv.db = MakeDb()
	kv.notifyChans = make(map[int]chan ChanResult)
	kv.LastClientOperation = make(map[int64]ClientOperation)

	kv.restoreFromSnapshotWithoutLock(persister.ReadSnapshot())
	go kv.applier()
	// kv.readPersist(persister.ReadSnapshot())
	// kv.readSnapshot(kv.persister.ReadSnapshot())
	// go kv.applyListener()
	return kv
}
