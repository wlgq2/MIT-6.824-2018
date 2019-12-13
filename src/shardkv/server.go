package shardkv


// import "shardmaster"
import "labrpc"
import "raft"
import "sync"
import "labgob"
import "log"
import "time"
import "bytes"
import "shardmaster"

type Op struct {
	Ch  chan (interface{})
	Req interface{}
}

var shardKvOnce sync.Once

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	kvs            [shardmaster.NShards] map[string]string
	msgIDs         map[int64] int64
	killChan       chan (bool)
	persister      *raft.Persister
	logApplyIndex  int
	
	config         shardmaster.Config
	mck            *shardmaster.Clerk
	timer    *time.Timer         
	EnableDebugLog bool
}

func (kv *ShardKV) println(args ...interface{}) {
	if kv.EnableDebugLog {
		log.Println(args...)
	}
}
//判定重复请求
func (kv *ShardKV) isRepeated(client int64,msgId int64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rst := false
	index,ok := kv.msgIDs[client]
	if ok {
		rst = index >= msgId
	}
	kv.msgIDs[client] = msgId
	return rst
}
//获取新增shards
func (kv *ShardKV) getNewShards(config *shardmaster.Config) map[int]int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shards := make(map[int]int)
	if config.Num > kv.config.Num {
		if config.Num >1 {
			oldShards := GetGroupShards(&(kv.config.Shards),kv.gid) //旧该组分片
			newShards := GetGroupShards(&(config.Shards),kv.gid) //新该组分片
			for key,_ := range newShards {  //获取新增组
				_,ok := oldShards[key]
				if !ok {
					shards[key] = kv.config.Shards[key]
				}
			}
		}
		kv.println(kv.gid,kv.me,"update config",config.Num,":",shardmaster.GetShardsInfoString(&(config.Shards)))
		if len(shards) >0 {
			kv.println(kv.gid,kv.me,"new shards",config.Num,":",GetGroupShardsString(shards))
		}
		return shards
	}
	return shards
}

func (kv *ShardKV) setConfig(config *shardmaster.Config) bool  {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num > kv.config.Num {
		kv.config = *config
		return true
	}
	return false
}
//raft更新config
func (kv *ShardKV) startConfig(config *shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num > kv.config.Num {
		op := Op {
			Req : *config, //请求数据
			Ch : make(chan(interface{})), //日志提交chan
		}
		kv.rf.Start(op) //写入Raft
	}
}

//raft更新config
func (kv *ShardKV) startShardData(config *shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num > kv.config.Num {
		op := Op {
			Req : *config, //请求数据
			Ch : make(chan(interface{})), //日志提交chan
		}
		kv.rf.Start(op) //写入Raft
	}
}

//判定重复请求
func (kv *ShardKV) isTrueGroup(shard int) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num == 0 {
		return false
	}
	return kv.config.Shards[shard] == kv.gid
}

//raft操作
func (kv *ShardKV) opt(req interface{}) (bool,interface{}) {
	op := Op {
		Req : req, //请求数据
		Ch : make(chan(interface{}),1), //日志提交chan
	}
	_, _, isLeader := kv.rf.Start(op) //写入Raft
	if !isLeader {  //判定是否是leader
		return false,nil  
	}
	select {
		case resp := <-op.Ch:
			return true,resp
		case <-time.After(time.Millisecond * 1000): //超时
	}
	return false,nil
}

func (kv *ShardKV) Get(req *GetArgs, reply *GetReply) {
	ok,value := kv.opt(*req)
	reply.WrongLeader = !ok
	if ok {
		*reply = value.(GetReply)
	}
}

func (kv *ShardKV) PutAppend(req *PutAppendArgs, reply *PutAppendReply) {
	ok,value := kv.opt(*req)
	reply.WrongLeader = !ok
	if ok {
		reply.Err = value.(Err)
	}
}

func (kv *ShardKV) GetShard(req *ReqShared, reply *RespShared) {
	ok,value := kv.opt(*req)
	reply.Shard = -1 
	if ok {
		*reply = value.(RespShared)
	}
}

func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	kv.killChan <- true
}

//判定是否写入快照
func  (kv *ShardKV) ifSaveSnapshot() {
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
//更新快照
func  (kv *ShardKV) updateSnapshot(data []byte) {
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
//写操作
func (kv *ShardKV) putAppend(req *PutAppendArgs) Err {
	if !kv.isTrueGroup(req.Shard) {
		return ErrWrongGroup
	}
	if !kv.isRepeated(req.Me,req.MsgId) { //去重复
		//kv.println(kv.me,"on",req.Op,req.Shard,req.Key,":",req.Value)
		if req.Op == "Put" {
			kv.kvs[req.Shard][req.Key] = req.Value
		} else if req.Op == "Append" {
			value, ok := kv.kvs[req.Shard][req.Key]
			if !ok {
				value = ""
			}
			value += req.Value
			kv.kvs[req.Shard][req.Key] = value
		}
	}
	return OK
}
//读操作
func (kv *ShardKV) get(req *GetArgs) (resp GetReply) {
	if !kv.isTrueGroup(req.Shard) {
		resp.Err = ErrWrongGroup
		return 
	}
	value, ok := kv.kvs[req.Shard][req.Key]
	resp.WrongLeader = false
	resp.Err = OK
	if !ok {
		value = ""
		resp.Err = ErrNoKey
	}
	resp.Value = value
	//kv.println(kv.me,"on get",req.Shard,req.Key,":",value)
	return 
}
func (kv *ShardKV) onGetShard(req *ReqShared) (resp RespShared) {
	if req.Config.Num > kv.config.Num { //自己未获取最新数据
		resp.Shard = -1 
		kv.timer.Reset(0) //获取最新数据
		return 
	}
	//复制已处理消息
	resp.MsgIDs = make(map[int64] int64)
	for key,value := range kv.msgIDs {
		resp.MsgIDs[key] = value
	}
	//复制分片数据
	resp.Data = make(map[string]string)
	data := kv.kvs[req.Shard]
	for key,value := range data{
		resp.Data[key] = value	
	}
	return 
}
//请求新增分片数据
func (kv *ShardKV) reqNewShardData(shards map[int]int,config *shardmaster.Config) {
	var wait sync.WaitGroup
	wait.Add(len(shards))
	for key,value := range shards {  //遍历所有分片，请求数据
		go func(shard int,group int){
			servers, ok := kv.config.Groups[group]  //获取目标组服务
			if ok {
				req := ReqShared{
					Config : *config,
					Shard : shard,
				}
				complet := false
				for !complet {
					for i := 0; i < len(servers); i++ {
						server := kv.make_end(servers[i])

						var reply RespShared
						ok := server.Call("ShardKV.GetShard", &req, &reply)
						if ok && reply.Shard == shard{
							//更新数据
							kv.kvs[shard] = reply.Data
							//更新msgid
							kv.mu.Lock()
							for key,value := range reply.MsgIDs {
								id,ok := kv.msgIDs[key]
								if !ok || id < value {
									kv.msgIDs[key] = value
								}
							}
							kv.mu.Unlock()
							complet = true
							break
						}
					}
				}
			}
			wait.Done()
		}(key,value)
	}
	wait.Wait()
}

func (kv *ShardKV) onConfig(config *shardmaster.Config) {
	shards := kv.getNewShards(config) //获取新增分片
	kv.reqNewShardData(shards,config)    //请求其他组数据
	kv.setConfig(config)
}
//apply 状态机
func (kv *ShardKV) onApply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid {  //非状态机apply消息
		if command, ok := applyMsg.Command.(raft.LogSnapshot); ok { //更新快照消息
			kv.updateSnapshot(command.Datas)
		} 
		kv.ifSaveSnapshot()
		return
	}
	//更新日志索引，用于创建最新快照
	kv.logApplyIndex = applyMsg.CommandIndex
	opt := applyMsg.Command.(Op)
	var resp interface{}
	if command, ok := opt.Req.(PutAppendArgs); ok { //Put && append操作
		resp = kv.putAppend(&command)
	} else if command, ok := opt.Req.(GetArgs); ok { //Get操作
		resp = kv.get(&command)
	} else if command, ok := opt.Req.(shardmaster.Config);ok {  //更新config
		kv.onConfig(&command)
	} else if command, ok := opt.Req.(ReqShared);ok {  //更新config
		resp = kv.onGetShard(&command)
	}
	select {
		case opt.Ch <- resp :
		default:
	}	
	kv.ifSaveSnapshot()
}

//获取新配置
func (kv *ShardKV) updateConfig()  {
	if _, isLeader := kv.rf.GetState(); isLeader {
		config := kv.mck.Query(kv.config.Num+1)
		kv.startConfig(&config)
	}
}

//轮询
func (kv *ShardKV) mainLoop() {
	duration := time.Duration(time.Millisecond * 100)

	//kv.getConfig()
	for {
		select {
		case <-kv.killChan:
			return
		case msg := <-kv.applyCh:
			if cap(kv.applyCh) - len(kv.applyCh) < 5 {
				log.Println(kv.me,"warn : maybe dead lock...")
			}
			kv.onApply(msg)
		case <-kv.timer.C :
			kv.updateConfig()
			kv.timer.Reset(duration)
		}
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	
	for i:=0;i<shardmaster.NShards;i++ {
		kv.kvs[i] = make(map[string]string)
	}
	kv.msgIDs = make(map[int64]int64)
	kv.killChan = make(chan (bool))
	kv.persister = persister
	kv.EnableDebugLog = -1==me
	kv.logApplyIndex = 0
	shardKvOnce.Do(func() {
		labgob.Register(Op{})
		labgob.Register(PutAppendArgs{})
		labgob.Register(GetArgs{})
		labgob.Register(ReqShared{})
		labgob.Register(RespShared{})
		labgob.Register(ReqDeleteShared{})
		labgob.Register(RespDeleteShared{})
		labgob.Register(shardmaster.Config{})
	})
	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.rf.EnableDebugLog = false
	kv.EnableDebugLog = 0==me
	kv.config.Num = 0
	kv.timer = time.NewTimer(time.Duration(time.Millisecond * 100))
	go kv.mainLoop()
	return kv
}
