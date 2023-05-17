package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	ErrNone        = "ErrNone"

	OpRead   = 1
	OpPut    = 2
	OpAppend = 3

	ExecuteTimeout = 1 * time.Second
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key     string
	Value   string
	Op      string // "Put" or "Append"
	ID      int
	Version int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

}

type Command struct {
	Key   string
	Value string
	Op    int
}

type ClientRequestArgs struct {
	ClientId    int64
	SequenceNum int
	Command     Command
}

type ClientRequestReply struct {
	Status   bool
	Response string
	Err      Err
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
