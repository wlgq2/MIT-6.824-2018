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
	Ch  chan (bool)
	Req PutAppendArgs
}

var kvOnce sync.Once
var KVServerEnableLog bool

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kvs      map[string]string
	killChan chan (bool)
}

func (kv *KVServer) println(args ...interface{}) {
	if KVServerEnableLog {
		log.Println(args...)
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.WrongLeader = false
	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
		reply.Err = ""
	} else {
		reply.Value = ""
		reply.Err = "Not find this key."
	}
	kv.println("on get",args.Key,":",reply.Value)
}

func (kv *KVServer) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	command := PutAppendCommand{
		Ch:  make(chan (bool)),
		Req: *req,
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
	case <-time.After(time.Millisecond * 2000):
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

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.killChan <- true
}

func (kv *KVServer) onApply(applyMsg raft.ApplyMsg) {
	command := applyMsg.Command.(PutAppendCommand)
	kv.putAppend(&command.Req)
	select {
	case command.Ch <- true:
	default:
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
	go kv.mainLoop()

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//kv.rf.EnableDebugLog = true
	kv.kvs = make(map[string]string)
	kv.killChan = make(chan (bool))
	KVServerEnableLog = false

	kvOnce.Do(func() {
		labgob.Register(PutAppendCommand{})
		labgob.Register(GetCommand{})
		log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	})
	return kv
}
