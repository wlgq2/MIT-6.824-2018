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
}

func (sm *ShardMaster) getCurrentConfig() Config {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) getConfig(index int, config *Config) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if index >0 && index < len(sm.configs) {
		*config = sm.configs[index]
		return true
	}
	return false
}

func (sm *ShardMaster) appendConfig(config *Config) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	config.Num = len(sm.configs)
	sm.configs = append(sm.configs, *config)
}

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

func (sm *ShardMaster) opt(client int64,msgId int64,req interface{}) (bool,interface{}) {
	if msgId > 0 && sm.isRepeated(client,msgId,false) {
		return true,nil
	}
	op := Op {
		Command : req,
		Ch : make(chan(interface{})),
	}
	_, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false,nil
	}
	select {
	case resp := <-op.Ch:
		return true,resp
	case <-time.After(time.Millisecond * 800): //超时
		
	}
	return false,nil
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	ok,_ := sm.opt(args.Me,args.MsgId,*args)
	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	ok,_ := sm.opt(args.Me,args.MsgId,*args)
	reply.WrongLeader = !ok
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	ok,_ := sm.opt(args.Me,args.MsgId,*args)
	reply.WrongLeader = !ok
}

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

func (sm *ShardMaster) join(args *JoinArgs ) bool {
	if !sm.isRepeated(args.Me,args.MsgId,true) { 
		return true
	}
	config := sm.getCurrentConfig()
	if config.Num == 0 {
		config.Groups = args.Servers
		DistributionGroups(&config)
	} else {
		MergeGroups(&config, args.Servers)
	}
	sm.appendConfig(&config)
	return  true
}

func (sm *ShardMaster) leave(args *LeaveArgs) bool {
	if !sm.isRepeated(args.Me,args.MsgId,true) { 
		return true
	}
	return true
}

func (sm *ShardMaster) move(args *MoveArgs,) bool{
	if !sm.isRepeated(args.Me,args.MsgId,true) { 
		return true
	}
	return true
}

func (sm *ShardMaster) query(args *QueryArgs) Config{
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
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	sm.applyCh = make(chan raft.ApplyMsg, 1024)
	sm.msgIDs = make(map[int64]int64)
	sm.killChan = make(chan (bool))
	smOnce.Do(func() {
		labgob.Register(Op{})
	})
	go sm.mainLoop()
	//sm.rf.EnableDebugLog = true
	return sm
}
