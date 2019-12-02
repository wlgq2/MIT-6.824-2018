package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

type RaftCommand struct {
	ch  chan (bool)
	req PutAppendArgs
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
	// Your definitions here.
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
}

func (kv *KVServer) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	command := RaftCommand{
		ch:  make(chan (bool)),
		req: *req,
	}
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error leader."
	}
	reply.WrongLeader = false
	select {
	case <-command.ch:
		reply.Err = ""
	case <-time.After(time.Millisecond * 1000):
		reply.Err = "timeout"
	}

}

func (kv *KVServer) putAppend(req *PutAppendArgs) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
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
	command := applyMsg.Command.(RaftCommand)
	kv.putAppend(&command.req)
	select {
	case command.ch <- true:
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
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	go kv.mainLoop()

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.EnableDebugLog = true
	kv.kvs = make(map[string]string)
	kv.killChan = make(chan (bool))
	KVServerEnableLog = true

	kvOnce.Do(func() {
		log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
		labgob.Register(RaftCommand{})
	})
	return kv
}
