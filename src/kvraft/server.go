package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

type PutAppendCommand struct {
	Ch  chan (bool)
	Req PutAppendArgs
}

type GetCommand struct {
	Ch  chan (GetReply)
	Req GetArgs
}

var kvOnce sync.Once


type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kvs            map[string]string
	msgIDs      map[int64] int64
	killChan    chan (bool)
	EnableDebugLog bool
}

func (kv *KVServer) println(args ...interface{}) {
	if kv.EnableDebugLog {
		log.Println(args...)
	}
}

func (kv *KVServer) Get(req *GetArgs, reply *GetReply) {
	command := GetCommand{
		Ch:  make(chan (GetReply)),
		Req: *req,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error leader."
	}
	reply.WrongLeader = false
	select {
	case *reply = <-command.Ch:
	case <-time.After(time.Millisecond * 700):
		reply.Err = "timeout"
	}
}

func (kv *KVServer) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	command := PutAppendCommand{
		Ch:  make(chan (bool)),
		Req: *req ,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error leader."
	}
	reply.WrongLeader = false
	select {
	case <-command.Ch:
		reply.Err = ""
	case <-time.After(time.Millisecond * 700):
		reply.Err = "timeout"
	}

}

func (kv *KVServer) putAppend(req *PutAppendArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.println("on",req.Op,req.Key,":",req.Value)
	if req.Op == "Put" {
		kv.kvs[req.Key] = req.Value
	} else if req.Op == "Append" {
		value, ok := kv.kvs[req.Key]
		if !ok {
			value = ""
		}
		value += req.Value
		kv.kvs[req.Key] = value
	}
}

func (kv *KVServer) get(args *GetArgs) (reply GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.WrongLeader = false
	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
		reply.Err = ""
	} else {
		reply.Value = ""
		reply.Err = ""
	}
	kv.println("on get",args.Key,":",reply.Value)
	return 
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.killChan <- true
}

func (kv *KVServer) isRepeated(req *PutAppendArgs) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	index,ok := kv.msgIDs[req.Me]
	if ok {
		if index < req.MsgId {
			kv.msgIDs[req.Me] = req.MsgId
			return false
		}
		return true
	}
	kv.msgIDs[req.Me] = req.MsgId
	return false
}

func (kv *KVServer) onApply(applyMsg raft.ApplyMsg) {
	if command, ok := applyMsg.Command.(PutAppendCommand); ok {
		if !kv.isRepeated(&command.Req) {
			kv.putAppend(&command.Req)
		}
		select {
		case command.Ch <- true:
		default:
		}
	} else {
		command := applyMsg.Command.(GetCommand)
		select {
		case command.Ch <- kv.get(&command.Req):
		default:
		}	
	}
}

func (kv *KVServer) mainLoop() {
	for {
		select {
		case <-kv.killChan:
			return
		case msg := <-kv.applyCh:
			kv.onApply(msg)
		}
	}
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.kvs = make(map[string]string)
	kv.msgIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool))
	kv.EnableDebugLog = false
	kvOnce.Do(func() {
		labgob.Register(PutAppendCommand{})
		labgob.Register(GetCommand{})
		log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	})
	go kv.mainLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.EnableDebugLog = false
	return kv
}
