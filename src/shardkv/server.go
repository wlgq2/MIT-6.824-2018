package shardkv

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
	killed         bool
	persister      *raft.Persister
	logApplyIndex  int
	
	config         shardmaster.Config
	nextConfig     shardmaster.Config
	mck            *shardmaster.Clerk
	timer          *time.Timer         
	shardCh        chan GroupShards
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

//判定cofig更新完成
func (kv *ShardKV) cofigCompleted(Num int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return  kv.config.Num >= Num
}
//获取新增shards
func (kv *ShardKV) getNewShards(config *shardmaster.Config) map[int][]int {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shards := make(map[int][]int)
	if config.Num > kv.config.Num {
		if config.Num >1 {
			oldShards := GetGroupShards(&(kv.config.Shards),kv.gid) //旧该组分片
			newShards := GetGroupShards(&(config.Shards),kv.gid) //新该组分片
			for key,_ := range newShards {  //获取新增组
				_,ok := oldShards[key]
				if !ok {
					group := kv.config.Shards[key]
					value,ok := shards[group]
					if !ok {
						value = make([]int,0)
					}
					value = append(value,key)
					shards[group] = value
				}
			}
		}
		kv.nextConfig = *config
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
//raft更新shard
func (kv *ShardKV) startShard(shards *RespShareds) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.println(kv.gid,kv.me,"start shard ", kv.nextConfig.Num)
	if kv.config.Num < kv.nextConfig.Num {
		op := Op {
			Req : *shards, //请求数据
			Ch : make(chan(interface{})), //日志提交chan
		}
		kv.rf.Start(op) //写入Raft
	}
}
//raft更新config
func (kv *ShardKV) startConfig(config *shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num > kv.nextConfig.Num && kv.nextConfig.Num == kv.config.Num {
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
	reply.Successed = false 
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
		kv.println(kv.gid,kv.me,"on",req.Op,req.Shard,"client",req.Me,"msgid:",req.MsgId,req.Key,":",req.Value)
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
func (kv *ShardKV) onSetShard(resp *RespShareds) {
	if kv.cofigCompleted(resp.ConfigNum) { //数据已更新
		return 
	}
	//更新数据
	for _,value := range resp.Shards {
		for shard,kvs := range value.Data {
			kv.kvs[shard] = kvs
		}
	}
	//更新msgid
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for _,shard := range resp.Shards {
		for key,value := range shard.MsgIDs {
			id,ok := kv.msgIDs[key]
			if !ok || id < value {
				//log.Println(kv.gid,kv.me,"update msg id",ok,key,id,value)
				kv.msgIDs[key] = value
			}
		}
	}
	kv.config = kv.nextConfig
}

func (kv *ShardKV) onGetShard(req *ReqShared) (resp RespShared) {
	if req.Config.Num > kv.config.Num { //自己未获取最新数据
		resp.Successed = false 
		kv.timer.Reset(0) //获取最新数据
		return 
	}
	resp.Successed = true
	//复制已处理消息
	resp.MsgIDs = make(map[int64] int64)
	for key,value := range kv.msgIDs {
		resp.MsgIDs[key] = value
	}
	//复制分片数据
	resp.Data = make(map[int]map[string]string)
	for i:=0;i<len(req.Shards);i++ {
		shard := req.Shards[i]
		data := kv.kvs[shard]
		shardDatas := make(map[string]string)
		for key,value := range data{
			shardDatas[key] = value	
		}		
		resp.Data[shard] = shardDatas
	}

	return 
}

func (kv *ShardKV) onConfig(config *shardmaster.Config) {
	if config.Num > kv.nextConfig.Num {
		shards := kv.getNewShards(config) //获取新增分片
		groupShards := GroupShards {
			Config : *config ,
			Shards : shards ,
		}
		kv.shardCh<-groupShards
	}
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
	} else if command, ok := opt.Req.(RespShareds);ok {  //更新config
		kv.onSetShard(&command)
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
		if kv.nextConfig.Num == kv.config.Num {
			config := kv.mck.Query(kv.nextConfig.Num+1)
			kv.startConfig(&config)
		}
	}
}

//轮询
func (kv *ShardKV) mainLoop() {
	duration := time.Duration(time.Millisecond * 100)
	for {
		select {
		case <-kv.killChan:
			kv.println(kv.gid,kv.me,"Exit mainLoop")
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

//请求其他组数据
func (kv *ShardKV) getShardLoop() {
	for !kv.killed {
		groupShards := <-kv.shardCh 
		Num := groupShards.Config.Num
		var wait sync.WaitGroup
		wait.Add(len(groupShards.Shards))
		rst := make(map[int]RespShared)
		var mutex sync.Mutex
		for key,value := range groupShards.Shards {  //遍历所有分片，请求数据
			go func(group int,shards []int) {
				defer wait.Done()
				servers, ok := kv.config.Groups[group]  //获取目标组服务
				if ok {
					req := ReqShared  {
						Config : kv.nextConfig,
						Shards : shards,
					}
					complet := false
					var reply RespShared
					for !complet && !(kv.cofigCompleted(Num)){
						for i := 0; i < len(servers); i++ {
							server := kv.make_end(servers[i])
							ok := server.Call("ShardKV.GetShard", &req, &reply)
							if ok && reply.Successed {
								complet = true
								break
							}
							if kv.cofigCompleted(Num) {
								break
							}
							time.Sleep(time.Millisecond*10)
						}
					}
					//存储该分片数据
					if !kv.cofigCompleted(Num) {
						mutex.Lock()
						rst[group] = reply
						mutex.Unlock()
					}
				}
				
			}(key,value)
		}
		wait.Wait()
		//获取的状态写入RAFT，直到成功
		for !(kv.cofigCompleted(Num)) {
			respShards := RespShareds{
				ConfigNum : Num ,
				Shards : rst,
			} 
			kv.startShard(&respShards)
			time.Sleep(time.Millisecond*1000)
		}
	}
	
}

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
		labgob.Register(RespShareds{})
		labgob.Register(ReqDeleteShared{})
		labgob.Register(RespDeleteShared{})
		labgob.Register(shardmaster.Config{})
	})
	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.rf.EnableDebugLog = false
	kv.EnableDebugLog = true
	kv.config.Num = 0
	kv.nextConfig.Num = 0
	kv.killed = false
	kv.shardCh = make(chan GroupShards)
	kv.timer = time.NewTimer(time.Duration(time.Millisecond * 100))
	go kv.mainLoop()
	go kv.getShardLoop()
	return kv
}
