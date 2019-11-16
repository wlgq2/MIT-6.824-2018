package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"
import "time"
import "sync/atomic"
import "math/rand"
import "fmt"

// import "bytes"
// import "labgob"

//节点状态
const Fallower, Leader, Candidate int = 1, 2, 3

//心跳超时阈值
const HeartbeatTimeoutCnt int64 = 3

//心跳周期
const HeartbeatDuration = time.Duration(time.Millisecond * 350)

//竞选周期随机范围，毫秒。
const CandidateDuration int = 300

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//日志
type LogEntry struct {
	Term  int
	Index int
	Log   interface{}
}

//投票请求
type RequestVoteArgs struct {
	Me           int
	ElectionTerm int
	LogIndex     int
	LogTerm      int
}

//投票rpc返回
type RequestVoteReply struct {
	IsAgree     bool
	CurrentTerm int
}

//日志复制请求
type AppendEntries struct {
	Me           int
	Term         int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

//回复日志更新请求
type RespEntries struct {
	Term      int
	Successed bool
}

type RespStart struct {
	term     int
	index    int
	isLeader bool
}

type Raft struct {
	//所有的状态改变及log增减都在MainLoop协程执行，所以不需要锁
	//mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // rpc节点
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // 自己服务编号
	logs           []LogEntry          // 日志存储
	logIndexs      map[int]int         //记录日志所在位置索引，用于减少查找日志时间复杂度，空间换时间
	commitIndex    int                 //当前日志提交处
	lastApplied    int                 //当前状态机执行处
	heartBeatCnt   int64               // 心跳计数
	status         int                 //节点状态
	currentTerm    int                 //当前周期
	heartbeatTimer *time.Timer         //心跳定时器
	candidateTimer *time.Timer         //竞选超时定时器
	randtime       *rand.Rand          //随机数，用于随机竞选周期，避免节点间竞争。

	nextIndex []int //记录每个fallow的日志状态

	applyCh          chan ApplyMsg
	killChan         chan (int)              //退出节点
	voteChan         chan (RequestVoteArgs)  //投票
	voteRstChan      chan (RequestVoteReply) //投票结果
	onEntriesChan    chan (AppendEntries)    //被leader同步日志
	onEntriesRstChan chan (RespEntries)      //同步日志结果
	startLogChan     chan (interface{})      //试图start log
	respStartLogChan chan (RespStart)
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rstChan := make(chan (bool))
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestVote", args, reply)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(time.Millisecond * 750):
		//rpc调用超时
	}
	return ok
}

func (rf *Raft) sendAppendEnteries(server int, req *AppendEntries, resp *RespEntries) bool {
	rstChan := make(chan (bool))
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestAppendEntries", req, resp)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(HeartbeatDuration):
		//rpc调用超时
	}
	return ok
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = rf.currentTerm
	isleader = rf.status == Leader
	return term, isleader
}

func (rf *Raft) setStatus(status int) {
	//设置节点状态，变换为fallow时候重置选举定时器
	if (rf.status != Fallower) && (status == Fallower) {
		rf.resetCandidateTimer()
	}

	//节点变为leader，则初始化fallow日志状态
	if rf.status != Leader && status == Leader {
		size := len(rf.peers)
		index := len(rf.logs)
		for i := 0; i < size; i++ {
			if (len(rf.nextIndex)) <= i {
				rf.nextIndex = append(rf.nextIndex, index)
			} else {
				rf.nextIndex[i] = index
			}
		}
	}
	rf.status = status

}

//获取日志索引及任期
func (rf *Raft) getLogTermAndIndex() (int, int) {
	index := 0
	term := 0
	size := len(rf.logs)
	if size > 0 {
		index = rf.logs[size-1].Index
		term = rf.logs[size-1].Term
	}
	return term, index
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.voteChan <- *args
	*reply = <-rf.voteRstChan
}

func (rf *Raft) onVote(args *RequestVoteArgs) RequestVoteReply {
	reply := RequestVoteReply{
		IsAgree:     true,
		CurrentTerm: rf.currentTerm,
	}
	//竞选任期小于自身任期，则反对票
	if rf.currentTerm >= args.ElectionTerm {
		reply.IsAgree = false
		return reply
	}
	//竞选任期大于自身任期，则更新自身任期，并转为fallow
	rf.setStatus(Fallower)
	rf.currentTerm = args.ElectionTerm

	logterm, logindex := rf.getLogTermAndIndex()
	//判定竞选者日志是否更新
	if logterm > args.LogTerm {
		reply.IsAgree = false
	} else if logterm == args.LogTerm {
		reply.IsAgree = logindex <= args.LogIndex
	}
	if reply.IsAgree {
		//赞同票后重置心跳（避免竞选竞争，增加一次选举成功的概率）
		rf.heartBeatCnt = 0
	}
	return reply
}

func (rf *Raft) Vote() {
	//投票先增大自身任期
	rf.currentTerm++
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), "start vote :", rf.me, "term :", rf.currentTerm)
	logterm, logindex := rf.getLogTermAndIndex()
	req := RequestVoteArgs{
		Me:           rf.me,
		ElectionTerm: rf.currentTerm,
		LogTerm:      logterm,
		LogIndex:     logindex,
	}
	var wait sync.WaitGroup
	peercnt := len(rf.peers)
	wait.Add(peercnt)
	agreeVote := 0
	term := rf.currentTerm
	for i := 0; i < peercnt; i++ {
		//并行调用投票rpc，避免单点阻塞
		go func(index int) {
			defer wait.Done()
			resp := RequestVoteReply{false, -1}
			if index == rf.me {
				agreeVote++
				return
			}
			rst := rf.sendRequestVote(index, &req, &resp)
			if !rst {
				return
			}
			if resp.IsAgree {
				agreeVote++
				return
			}
			if resp.CurrentTerm > term {
				term = resp.CurrentTerm
			}

		}(i)
	}
	wait.Wait()
	//如果存在系统任期更大，则更像任期并转为fallow
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.setStatus(Fallower)
		return
	}
	//获得多数赞同则变成leader
	if agreeVote*2 > peercnt {
		fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), rf.me, "become leader.")
		rf.setStatus(Leader)
		rf.appendEntries()
	}
}

func (rf *Raft) RequestAppendEntries(req *AppendEntries, resp *RespEntries) {
	rf.onEntriesChan <- *req
	*resp = <-rf.onEntriesRstChan
}

func (rf *Raft) updateLog(index int, log LogEntry) {
	if index < len(rf.logs) {
		rf.logs[index] = log
	} else {
		rf.logs = append(rf.logs, log)
	}
	rf.logIndexs[log.Index] = index
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), rf.me, " update log ", index, ":", log.Term, "-", log.Index)
}

func (rf *Raft) OnAppendEntries(req *AppendEntries) RespEntries {
	resp := RespEntries{
		Term:      rf.currentTerm,
		Successed: true,
	}
	if req.Term < rf.currentTerm {
		//leader任期小于自身任期，则拒绝同步log
		resp.Successed = false
		return resp
	}
	//否则更新自身任期，切换自生为fallow，清空心跳超时计数
	rf.currentTerm = req.Term
	rf.setStatus(Fallower)
	atomic.StoreInt64(&rf.heartBeatCnt, 0)
	//判定与leader日志是一致
	index := -1
	ok := true
	if req.PrevLogIndex >= 0 {
		if index, ok = rf.logIndexs[req.PrevLogIndex]; !ok {
			//改索引在自身日志中不存在，则拒绝更新
			fmt.Println(rf.me, "can't find index", req.PrevLogIndex, "in maps")
			resp.Successed = false
			return resp
		}
		if rf.logs[index].Term != req.PrevLogTerm {
			//该索引与自身日志不同，则拒绝更新
			fmt.Println(rf.me, "term error", req.PrevLogTerm, rf.logs[index].Term)
			resp.Successed = false
			return resp
		}
	}
	//更新日志
	size := len(req.Entries)
	for i := 0; i < size; i++ {
		rf.updateLog(index+i+1, req.Entries[i])
	}
	rf.logs = rf.logs[:(index + size + 1)]
	rf.commitIndex = req.LeaderCommit
	rf.apply()
	return resp

}

func (rf *Raft) getEntriesInfo(index int, entries *[]LogEntry) (preterm int, preindex int) {
	pre := index - 1
	//即fallow 日志为空
	if pre < 0 {
		preterm = -1
		preindex = -1
	} else {
		preindex = rf.logs[pre].Index
		preterm = rf.logs[pre].Term
	}
	for i := index; i < len(rf.logs); i++ {
		*entries = append(*entries, rf.logs[i])
	}
	return
}

//apply 状态机
func (rf *Raft) apply() {
	_, last := rf.getLogTermAndIndex()
	for ; rf.lastApplied < rf.commitIndex && rf.lastApplied < last; rf.lastApplied++ {
		if index, ok := rf.logIndexs[rf.lastApplied+1]; ok {
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[index].Log,
				CommandIndex: rf.logs[index].Index,
			}
			fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), rf.me, "apply log", rf.lastApplied+1, "-", rf.logs[index].Index)
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) waitForAppendEntries(servers []int) []int {
	var successPeers []int
	term := 0
	count := len(servers)
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func(index int) {
			defer wg.Done()
			if index == rf.me {
				successPeers = append(successPeers, index)
				return
			}

			for {
				req := AppendEntries{
					Me:           rf.me,
					Term:         rf.currentTerm,
					LeaderCommit: rf.commitIndex,
				}
				resp := RespEntries{Term: 0}
				//当前fallow的日志状态
				next := rf.nextIndex[index]
				req.PrevLogTerm, req.PrevLogIndex = rf.getEntriesInfo(next, &req.Entries)
				if len(req.Entries) > 0 {
					fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), "replicate log to ", index, " preterm", req.PrevLogTerm, " preindex", req.PrevLogIndex)
				}

				rst := rf.sendAppendEnteries(index, &req, &resp)
				if rst {
					//如果某个节点任期大于自己，则更新任期，变成fallow
					if resp.Term > rf.currentTerm {
						if resp.Term > term {
							term = resp.Term
						}
						break
					}
					//如果更新失败则fallow日志状态减1
					if !resp.Successed {
						if rf.nextIndex[index] > 0 {
							rf.nextIndex[index]--
						} else {
							break
						}
					} else {
						//更新成功
						rf.nextIndex[index] += len(req.Entries)
						successPeers = append(successPeers, index)
						break
					}
				} else {
					break
				}
			}
		}(servers[i])
	}
	wg.Wait()
	//发现有更大term
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.setStatus(Fallower)
		successPeers = successPeers[0:0]
	}
	return successPeers
}

func (rf *Raft) appendEntries() {
	if rf.status != Leader {
		return
	}
	var input []int
	for i := 0; i < len(rf.peers); i++ {
		input = append(input, i)
	}
	//等待同步日志结果
	rst := rf.waitForAppendEntries(input)
	//日志提交成功
	if len(rst)*2 > len(rf.peers) {	
		_, rf.commitIndex = rf.getLogTermAndIndex()
		rf.apply()
		//通知更新成功的节点apply
		rf.waitForAppendEntries(rst)
	}
}

//心跳周期
func (rf *Raft) onHeartbeatTimerout() {
	if rf.status == Fallower {
		//心跳计数+1
		atomic.AddInt64(&rf.heartBeatCnt, 1)
	} else if rf.status == Leader {
		rf.appendEntries()
		atomic.StoreInt64(&rf.heartBeatCnt, 0)
	} else {
		atomic.StoreInt64(&rf.heartBeatCnt, 0)
	}
	rf.heartbeatTimer.Reset(HeartbeatDuration)
}

func (rf *Raft) onCandidateTimeout() {
	if rf.status == Candidate {
		//如果状态为竞选者，则直接发动投票
		rf.Vote()
		rf.resetCandidateTimer()
	} else if rf.status == Fallower {
		//如果心跳超时，则fallow转变为candidata并发动投票
		if rf.heartBeatCnt >= HeartbeatTimeoutCnt {
			rf.setStatus(Candidate)
			rf.Vote()
		}
		rf.resetCandidateTimer()
	}
}

//重置竞选周期定时
func (rf *Raft) resetCandidateTimer() {
	randCnt := rf.randtime.Intn(CandidateDuration)
	cnt := time.Duration(randCnt)*time.Millisecond + time.Duration(HeartbeatTimeoutCnt)*HeartbeatDuration
	duration := time.Duration(cnt)
	rf.candidateTimer.Reset(duration)
}

func (rf *Raft) insertLog(command interface{}) int {
	entry := LogEntry{
		Term:  rf.currentTerm,
		Index: -1,
		Log:   command,
	}
	//获取log索引，并插入map中
	_, index := rf.getLogTermAndIndex()
	index++
	entry.Index = index
	rf.logIndexs[index] = len(rf.logs)
	//插入log
	rf.logs = append(rf.logs, entry)
	return index
}

func (rf *Raft) startLog(command interface{}) RespStart {
	resp := RespStart{
		term:     -1,
		index:    -1,
		isLeader: rf.status == Leader,
	}

	if resp.isLeader {
		//设置term并插入log
		resp.term = rf.currentTerm
		resp.index = rf.insertLog(command)
		fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), "leader", rf.me, ":", "append log", resp.term, "-", resp.index)
	}
	return resp
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.startLogChan <- command
	rst := <-rf.respStartLogChan
	return rst.index, rst.term, rst.isLeader
}

func (rf *Raft) Kill() {
	close(rf.killChan)
}

//主轮询loop
func (rf *Raft) MainLoop() {
	//心跳定时器
	rf.heartbeatTimer = time.NewTimer(HeartbeatDuration)
	//选举超时定时器
	rf.candidateTimer = time.NewTimer(time.Duration(0))
	rf.resetCandidateTimer()
	defer func() {
		rf.heartbeatTimer.Stop()
		rf.candidateTimer.Stop()
	}()

	for {
		select {
		case <-rf.heartbeatTimer.C:
			rf.onHeartbeatTimerout()
		case <-rf.candidateTimer.C:
			rf.onCandidateTimeout()
		case req := <-rf.voteChan:
			rf.voteRstChan <- rf.onVote(&req)
		case req := <-rf.onEntriesChan:
			rf.onEntriesRstChan <- rf.OnAppendEntries(&req)
		case args := <-rf.startLogChan:
			rf.respStartLogChan <- rf.startLog(args)
		case <-rf.killChan:
			return
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.status = Fallower
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.randtime = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.logIndexs = make(map[int]int)
	rf.killChan = make(chan (int))
	rf.voteChan = make(chan (RequestVoteArgs))
	rf.voteRstChan = make(chan (RequestVoteReply))
	rf.onEntriesChan = make(chan (AppendEntries))
	rf.onEntriesRstChan = make(chan (RespEntries))
	rf.startLogChan = make(chan (interface{}))
	rf.respStartLogChan = make(chan (RespStart))
	//主循环
	go rf.MainLoop()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
