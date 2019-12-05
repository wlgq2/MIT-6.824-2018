package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
	"bytes"
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
	//mu      sync.Mutex  //数据操作都在mainLoop协程，不需要锁
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	kvs            map[string]string
	msgIDs         map[int64] int64
	killChan       chan (bool)
	persister      *raft.Persister
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
	kv.println(kv.me,"on",req.Op,req.Key,":",req.Value)
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
	reply.WrongLeader = false
	value, ok := kv.kvs[args.Key]
	if ok {
		reply.Value = value
		reply.Err = ""
	} else {
		reply.Value = ""
		reply.Err = ""
	}
	kv.println(kv.me,"on get",args.Key,":",reply.Value)
	return 
}

func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.killChan <- true
}

func (kv *KVServer) isRepeated(req *PutAppendArgs) bool {
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

func  (kv *KVServer) ifSaveSnapshot(index int) {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		writer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(writer)
		encoder.Encode(kv.msgIDs)
		encoder.Encode(kv.kvs)
		data := writer.Bytes()
		kv.rf.SaveSnapshot(index,data)
		return 
	}
}

func  (kv *KVServer) updateSnapshot(data [] byte) {
	if data == nil || len(data) < 1 {
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)

	if decoder.Decode(&kv.msgIDs) != nil ||
		decoder.Decode(&kv.kvs) != nil  {
		kv.println("Error in unmarshal raft state")
	} 
}



func (kv *KVServer) onApply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid {
		if command, ok := applyMsg.Command.(raft.LogSnapshot); ok {
			kv.updateSnapshot(command.Datas)
		}
		return
	}
	if command, ok := applyMsg.Command.(PutAppendCommand); ok {
		if !kv.isRepeated(&command.Req) {
			kv.putAppend(&command.Req)
			kv.ifSaveSnapshot(applyMsg.CommandIndex)
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

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg, 1024)
	kv.kvs = make(map[string]string)
	kv.msgIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool))
	kv.persister = persister
	kv.EnableDebugLog = false
	kvOnce.Do(func() {
		labgob.Register(PutAppendCommand{})
		labgob.Register(GetCommand{})
	})
	go kv.mainLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.EnableDebugLog = false
	return kv
}
