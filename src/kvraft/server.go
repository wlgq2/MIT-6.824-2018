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
	Ch  chan (string)
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
	msgIDs         map[int64] int64
	killChan       chan (bool)
	persister      *raft.Persister
	logApplyIndex  int
	EnableDebugLog bool
}

func (kv *KVServer) println(args ...interface{}) {
	if kv.EnableDebugLog {
		log.Println(args...)
	}
}

func (kv *KVServer) Get(req *GetArgs, reply *GetReply) {
	command := GetCommand{
		Ch:  make(chan (string)),
		Req: *req,
	}
	//写入Raft日志，等待数据返回
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error leader."
		return
	}
	reply.WrongLeader = false
	select {
	case reply.Value = <-command.Ch:
		reply.Err = ""
	case <-time.After(time.Millisecond * 1000): //超时
		reply.Err = "timeout"
	}
}

func (kv *KVServer) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	command := PutAppendCommand{
		Ch:  make(chan (bool)),
		Req: *req ,
	}
	//去重复
	if kv.isRepeated(req) {
		reply.WrongLeader = false
		reply.Err = ""
		return
	}
	//写入Raft日志，等待数据返回
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.WrongLeader = true
		reply.Err = "error leader."
		return 
	}
	reply.WrongLeader = false
	select {
	case <-command.Ch:
		reply.Err = ""
	case <-time.After(time.Millisecond * 700): //超时
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

func (kv *KVServer) get(args *GetArgs) (value string) {
	value, ok := kv.kvs[args.Key]
	if !ok {
		value = ""
	}
	kv.println(kv.me,"on get",args.Key,":",value)
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
		return index >= req.MsgId
	}
	return false
}

func (kv *KVServer) isRepeatedWithUpdate(req *PutAppendArgs) bool {
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

func  (kv *KVServer) ifSaveSnapshot() {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		writer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(writer)
		encoder.Encode(kv.msgIDs)
		encoder.Encode(kv.kvs)
		data := writer.Bytes()
		kv.rf.SaveSnapshot(kv.logApplyIndex,data)
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
	if !applyMsg.CommandValid {  //非状态机apply消息
		if command, ok := applyMsg.Command.(raft.LogSnapshot); ok { //更新快照消息
			kv.updateSnapshot(command.Datas)
		} 
		kv.ifSaveSnapshot()
		return
	}
	//更新日志索引，用于创建最新快照
	kv.logApplyIndex = applyMsg.CommandIndex
	if command, ok := applyMsg.Command.(PutAppendCommand); ok { //Put && append操作
		if !kv.isRepeatedWithUpdate(&command.Req) { //去重复
			kv.putAppend(&command.Req)
		}
		select {
		case command.Ch <- true:
		default:
		}
	} else { //Get操作
		command := applyMsg.Command.(GetCommand)
		select {
		case command.Ch <- kv.get(&command.Req):
		default:
		}	
	}
	kv.ifSaveSnapshot()
}

//轮询
func (kv *KVServer) mainLoop() {
	for {
		select {
		case <-kv.killChan:
			return
		case msg := <-kv.applyCh:
			if cap(kv.applyCh) - len(kv.applyCh) < 5 {
				log.Println("warn : maybe dead lock...")
			}
			kv.onApply(msg)
		}
	}
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.kvs = make(map[string]string)
	kv.msgIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool))
	kv.persister = persister
	kv.EnableDebugLog = false
	kv.logApplyIndex = 0
	kvOnce.Do(func() {
		labgob.Register(PutAppendCommand{})
		labgob.Register(GetCommand{})
	})
	go kv.mainLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.EnableDebugLog = false
	return kv
}
