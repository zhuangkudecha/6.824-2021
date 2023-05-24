package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId   int64
	commandId  int64
	lastLeader int

	mu *sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.commandId = 0
	ck.lastLeader = 0
	ck.mu = &sync.Mutex{}
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := CommandArgs{}
	args.Num = num
	args.Op = OpQuery
	return ck.Command(&args)
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := CommandArgs{}
	args.Servers = servers
	args.Op = OpJoin
	ck.Command(&args)
}

func (ck *Clerk) Leave(gids []int) {
	args := CommandArgs{}
	args.GIDs = gids
	args.Op = OpLeave
	ck.Command(&args)
}

func (ck *Clerk) Move(shard int, gid int) {
	args := CommandArgs{}
	args.Shard = shard
	args.GID = gid
	args.Op = OpMove
	ck.Command(&args)
}

func (ck *Clerk) Command(args *CommandArgs) Config {
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		var response CommandReply
		ok := ck.servers[ck.lastLeader].Call("ShardCtrler.Command", args, &response)
		if !ok || response.Err == ErrWrongLeader || response.Err == ErrTimeout {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		ck.commandId++
		DPrintf("client %v send command %v to server %v, got response %v", ck.clientId, args, ck.lastLeader, response)
		return response.Config
	}
}
