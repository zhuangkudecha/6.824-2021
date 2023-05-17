package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu *sync.Mutex
	// ID         int   // clerk id
	// lastServer int   // last server that the clerk connected to (leader id)
	// version    int64 // used to check if the command is duplicated
	lastLeader  int   // last leader that the clerk connected to
	clientID    int64 // random number
	sequenceNum int   // mon increase
}

var ID int = 0

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = int(nrand()) % len(servers)
	ck.clientID = nrand()
	ck.sequenceNum = 0
	ck.mu = &sync.Mutex{}
	return ck
}

func (ck *Clerk) sendCommand(command Command) string {
	// ck.mu.Lock()
	// defer ck.mu.Unlock()
	DPrintf("Client %v start to send command sequenceNum %v, %+v", ck.clientID, ck.sequenceNum, command)
	for {
		args := &ClientRequestArgs{
			Command:     command,
			ClientId:    ck.clientID,
			SequenceNum: ck.sequenceNum,
		}
		reply := &ClientRequestReply{}

		if ck.servers[ck.lastLeader].Call("KVServer.HandleCommand", args, reply) {
			if reply.Status {
				DPrintf("Client %v:Execute command success, sequenceNum %v, Command %+v", ck.clientID, ck.sequenceNum, command)
				ck.sequenceNum++
				return reply.Response
			} else {
				if reply.Err == ErrNone || reply.Err == ErrTimeout {
					continue
				}
			}
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		DPrintf("Client %v:Execute command %v failed, retry, change leader to %v", ck.clientID, command, ck.lastLeader)
		time.Sleep(500 * time.Millisecond)
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {
	command := Command{
		Key:   key,
		Value: "",
		Op:    OpRead,
	}
	return ck.sendCommand(command)
}

// func (ck *Clerk) Get(key string) string {
// 	args := GetArgs{Key: key}
// 	// You will have to modify this function.
// 	for i := 0; ; i++ {
// 		// ck.mu.Lock()
// 		// defer ck.mu.Unlock()
// 		reply := GetReply{}
// 		DPrintf("Sending Get RPC to %d\n", (i+ck.lastServer)%len(ck.servers))
// 		ok := ck.servers[(i+ck.lastServer)%len(ck.servers)].Call("KVServer.Get", &args, &reply)
// 		if !ok {
// 			continue
// 		}
// 		switch reply.Err {
// 		case OK:
// 			ck.lastServer = (i + ck.lastServer) % len(ck.servers)
// 			DPrintf("finsh get from server %d\n", (i+ck.lastServer)%len(ck.servers))
// 			return reply.Value

// 		case ErrNoKey:
// 			ck.lastServer = (i + ck.lastServer) % len(ck.servers)
// 			return ""

// 		case ErrWrongLeader:
// 			if i%len(ck.servers) == 0 {
// 				DPrintf("clerk get : traversed all servers but no one available. sleep 100ms\n")
// 				time.Sleep(100 * time.Millisecond)
// 			}
// 			continue
// 		}
// 	}
// }

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op int) {
	command := Command{
		Key:   key,
		Value: value,
		Op:    op,
	}
	ck.sendCommand(command)
}

// func (ck *Clerk) PutAppend(key string, value string, op string) {
// 	// You will have to modify this function.
// 	args := PutAppendArgs{
// 		Key:     key,
// 		Value:   value,
// 		Op:      op,
// 		Version: ck.version,
// 		ID:      ck.ID,
// 	}
// 	ck.version++
// 	for i := 0; ; i++ {
// 		// ck.mu.Lock()
// 		// defer ck.mu.Unlock()
// 		reply := PutAppendReply{}
// 		DPrintf("Sending PutAppend RPC to %d\n", (i+ck.lastServer)%len(ck.servers))
// 		ok := ck.servers[(i+ck.lastServer)%len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)
// 		if !ok {
// 			continue
// 		}
// 		switch reply.Err {
// 		case OK:
// 			DPrintf("finsh putappend from server %d\n", (i+ck.lastServer)%len(ck.servers))
// 			ck.lastServer = (i + ck.lastServer) % len(ck.servers)
// 			return
// 		case ErrNoKey:
// 			DPrintf("No key\n")
// 			ck.lastServer = (i + ck.lastServer) % len(ck.servers)
// 			return
// 		case ErrWrongLeader:
// 			if i+ck.lastServer%len(ck.servers) == 0 {
// 				DPrintf("clerk putappend : traversed all servers but no one available. sleep 100ms\n")
// 				time.Sleep(200 * time.Millisecond)
// 			}
// 			continue
// 		}
// 	}

// }

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
