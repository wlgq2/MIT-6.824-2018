package shardmaster


import "raft"
import "labrpc"
import "sync"
import "labgob"
import "time"
import "log"

var smOnce sync.Once

type Op struct {
	Command interface{}
	Ch  chan (interface{})
}

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	msgIDs         map[int64] int64
	killChan       chan (bool)

	configs []Config // indexed by config num
	EnableDebugLog bool
}

func (sm *ShardMaster) println(args ...interface{}) {
	if sm.EnableDebugLog {
		log.Println(args...)
	}
}

//获取最新配置
func (sm *ShardMaster) getCurrentConfig() Config {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	config := sm.configs[len(sm.configs)-1]
	CopyGroups(&config,config.Groups)
	return config
}
//获取历史配置
func (sm *ShardMaster) getConfig(index int, config *Config) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if index >=0 && index < len(sm.configs) {
		*config = sm.configs[index]
		return true
	}
	*config = sm.configs[len(sm.configs)-1]
	return false
}
//更新配置
func (sm *ShardMaster) appendConfig(config *Config) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	config.Num = len(sm.configs)
	sm.configs = append(sm.configs, *config)
}
//判定重复请求
func (sm *ShardMaster) isRepeated(client int64,msgId int64,update bool) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	rst := false
	index,ok := sm.msgIDs[client]
	if ok {
		rst = index >= msgId
	}
	if update && !rst {
		sm.msgIDs[client] = msgId
	}
	return rst
}
//raft同步操作
func (sm *ShardMaster) opt(client int64,msgId int64,req interface{}) (bool,interface{}) {
	if msgId > 0 && sm.isRepeated(client,msgId,false) {
		return true,nil
	}
	op := Op {
		Command : req, //请求数据
		Ch : make(chan(interface{})), //日志提交chan
	}
	_, _, isLeader := sm.rf.Start(op) //写入Raft
	if !isLeader {
		return false,nil  //判定是否是leader
	}
	select {
	case resp := <-op.Ch:
		return true,resp
	case <-time.After(time.Millisecond * 800): //超时
	}
	return false,nil
}
//增加组
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	ok,_ := sm.opt(args.Me,args.MsgId,*args)
	reply.WrongLeader = !ok
}
//删除组
func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	ok,_ := sm.opt(args.Me,args.MsgId,*args)
	reply.WrongLeader = !ok
}
//移动组
func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	ok,_ := sm.opt(args.Me,args.MsgId,*args)
	reply.WrongLeader = !ok
}
//查询组分片配置
func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	if sm.getConfig(args.Num, &reply.Config) {
		reply.WrongLeader = false
		return 
	}
	ok,resp := sm.opt(-1,-1,*args)
	if ok {
		reply.Config = resp.(Config)
	}
	reply.WrongLeader = !ok
}


func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	sm.killChan <- true
}

func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) join(args *JoinArgs) bool {
	sm.println("on join ",GetGroupsInfoString(args.Servers))
	if sm.isRepeated(args.Me,args.MsgId,true) { //去重复
		return true
	}
	config := sm.getCurrentConfig() //最新配置
	if config.Num == 0{  //如果第一次配置，则重分配
		config.Groups = args.Servers
		DistributionGroups(&config) //重分配分片与组
	} else {
		MergeGroups(&config, args.Servers) //合并组
	}
	sm.appendConfig(&config)
	sm.println("after join ",GetShardsInfoString(&(config.Shards)))
	return  true
}

func (sm *ShardMaster) leave(args *LeaveArgs) bool {
	sm.println("on leave ",GetSlientInfoString(args.GIDs))
	if sm.isRepeated(args.Me,args.MsgId,true) { 
		return true
	}
	config := sm.getCurrentConfig()
	DeleteGroups(&config,args.GIDs) //删除组
	sm.appendConfig(&config)
	sm.println("after leave ",GetShardsInfoString(&(config.Shards)))
	return true
}

func (sm *ShardMaster) move(args *MoveArgs,) bool{
	if sm.isRepeated(args.Me,args.MsgId,true) { 
		return true
	}
	config := sm.getCurrentConfig()
	config.Shards[args.Shard] = args.GID
	sm.appendConfig(&config)
	sm.println("on move ",args.Shard,args.GID)
	sm.println("after move ",GetShardsInfoString(&(config.Shards)))
	return true
}

func (sm *ShardMaster) query(args *QueryArgs) Config {
	//sm.println("on query ",args.Num)
	reply := Config {}
	sm.getConfig(args.Num,&reply)
	return reply
}

func (sm *ShardMaster)onApply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid {  //非状态机apply消息
		return
	}
	op := applyMsg.Command.(Op)
	var resp interface{}
	if command, ok := op.Command.(JoinArgs); ok { 
		resp = sm.join(&command)
	} else if command, ok := op.Command.(LeaveArgs); ok  {
		resp = sm.leave(&command)
	} else if command, ok := op.Command.(MoveArgs); ok  { 
		resp = sm.move(&command)
	} else {
		command := op.Command.(QueryArgs)
		resp = sm.query(&command)
	}
	select {
		case op.Ch <- resp:
		default:
	}
}
//主循环
func (sm *ShardMaster)  mainLoop() {
	for {
		select {
		case <-sm.killChan:
			return
		case msg := <-sm.applyCh:
			if cap(sm.applyCh) - len(sm.applyCh) < 5 {
				log.Println("warn : maybe dead lock...")
			}
			sm.onApply(msg)
		}
	}
}


func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	smOnce.Do(func() {
		labgob.Register(Op{})
		labgob.Register(JoinArgs{})
		labgob.Register(LeaveArgs{})
		labgob.Register(MoveArgs{})
		labgob.Register(QueryArgs{})
	})
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	sm.applyCh = make(chan raft.ApplyMsg, 1024)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.msgIDs = make(map[int64]int64)
	sm.killChan = make(chan (bool))
	go sm.mainLoop()
	sm.EnableDebugLog = false
	sm.rf.EnableDebugLog = false
	return sm
}
