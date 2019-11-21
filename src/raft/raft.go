package raft

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "log"
import "bytes"
import "labgob"
import "runtime"
import "strconv"
import "sort"

//节点状态
const Fallower, Leader, Candidate int = 1, 2, 3

//心跳周期
const HeartbeatDuration = time.Duration(time.Millisecond * 1000)

//竞选周期
const CandidateDuration = HeartbeatDuration + time.Duration(time.Millisecond*400)

var raftOnce sync.Once

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

type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // rpc节点
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // 自己服务编号
	logs            []LogEntry          // 日志存储
	commitIndex     int                 //当前日志提交处
	lastApplied     int                 //当前状态机执行处
	status          int                 //节点状态
	currentTerm     int                 //当前周期
	heartbeatTimers []*time.Timer       //心跳定时器
	eletionTimer    *time.Timer         //竞选超时定时器
	randtime        *rand.Rand          //随机数，用于随机竞选周期，避免节点间竞争。

	nextIndex  []int         //记录每个fallow的同步日志状态
	matchIndex []int         //记录每个fallow日志最大索引，0递增
	applyCh    chan ApplyMsg //状态机apply
	isKilled   bool          //节点退出
}

func (rf *Raft) lock(info string) {
	//log.Println(GetGID(), rf.me, "try lock", info)
	rf.mu.Lock()
}

func (rf *Raft) unlock(info string) {
	//log.Println(GetGID(), rf.me, "try unlock", info)
	rf.mu.Unlock()
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
	case <-time.After(time.Millisecond * 700):
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
	case <-time.After(200):
		//rpc调用超时
	}
	return ok
}

func (rf *Raft) GetState() (int, bool) {
	rf.lock("Raft.GetState")
	defer rf.unlock("Raft.GetState")
	term := int(rf.currentTerm)
	isleader := rf.status == Leader
	return term, isleader
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

//获取协程ID
func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func (rf *Raft) persist() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.lastApplied)
	encoder.Encode(rf.logs)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var commitIndex, lastApplied, currentTerm int
	var logs []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&lastApplied) != nil ||
		decoder.Decode(&logs) != nil {
		log.Println("Error in unmarshal raft state")
	} else {

		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
		rf.logs = logs
		rf.apply()
	}

}

func (rf *Raft) setStatus(status int) {
	//设置节点状态，变换为fallow时候重置选举定时器
	if (rf.status != Fallower) && (status == Fallower) {
		rf.resetCandidateTimer()
	}

	//节点变为leader，则初始化fallow日志状态
	if rf.status != Leader && status == Leader {
		index := len(rf.logs)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = index + 1
			rf.matchIndex[i] = 0
		}
	}
	rf.status = status
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock("Raft.RequestVote")
	defer rf.unlock("Raft.RequestVote")
	reply.IsAgree = true
	reply.CurrentTerm = rf.currentTerm
	//竞选任期小于自身任期，则反对票
	if rf.currentTerm >= args.ElectionTerm {
		reply.IsAgree = false
		return
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
		//赞同票后重置选举定时，避免竞争
		rf.resetCandidateTimer()
	}
}

func (rf *Raft) Vote() {
	rf.lock("Raft.Vote")
	//投票先增大自身任期
	rf.currentTerm++
	log.Println("start vote :", rf.me, "term :", rf.currentTerm)
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
	rf.unlock("Raft.Vote")
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
	rf.lock("Raft.Vote")
	//如果存在系统任期更大，则更像任期并转为fallow
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.setStatus(Fallower)
	} else if agreeVote*2 > peercnt { //获得多数赞同则变成leader
		log.Println(rf.me, "become leader :", rf.currentTerm)
		rf.setStatus(Leader)
		rf.replicateLogNow()
	}
	rf.unlock("Raft.Vote")
}

func (rf *Raft) onElectionTimeout() {
	rf.lock("Raft.onElectionTimeout")
	if rf.status == Candidate {
		rf.unlock("Raft.onElectionTimeout")
		//如果状态为竞选者，则直接发动投票
		rf.Vote()
		rf.resetCandidateTimer()
	} else if rf.status == Fallower {
		//如果状态为fallow，则转变为candidata并发动投票
		rf.setStatus(Candidate)
		rf.unlock("Raft.onElectionTimeout")
		rf.Vote()
		rf.resetCandidateTimer()
	} else {
		rf.unlock("Raft.onElectionTimeout")
	}
}

//重置竞选周期定时
func (rf *Raft) resetCandidateTimer() {
	randCnt := rf.randtime.Intn(600)
	duration := time.Duration(randCnt)*time.Millisecond + CandidateDuration
	rf.eletionTimer.Reset(duration)
}

//主轮询loop
func (rf *Raft) ElectionLoop() {
	//选举超时定时器
	rf.resetCandidateTimer()
	defer rf.eletionTimer.Stop()

	for !rf.isKilled {
		<-rf.eletionTimer.C
		if rf.isKilled {
			break
		}
		rf.onElectionTimeout()
	}
	//rf.persist()
}

func (rf *Raft) updateLog(index int, logEntry LogEntry) {
	if index < len(rf.logs) {
		rf.logs[index] = logEntry
	} else {
		rf.logs = append(rf.logs, logEntry)
	}
	log.Println(rf.me, " update log ", index, ":", logEntry.Term, "-", logEntry.Index)
}

func (rf *Raft) getEntriesInfo(index int, entries *[]LogEntry) (preterm int, preindex int) {
	pre := index - 1
	if pre == 0 {
		preindex = 0
		preterm = 0
	} else {
		preindex = rf.logs[pre-1].Index
		preterm = rf.logs[pre-1].Term
	}
	for i := pre; i < len(rf.logs); i++ {
		*entries = append(*entries, rf.logs[i])
	}
	return
}

//获取当前已被提交日志
func (rf *Raft) updateCommitIndex() bool {
	rst := false
	var indexs []int
	_, rf.matchIndex[rf.me] = rf.getLogTermAndIndex()
	for i := 0; i < len(rf.matchIndex); i++ {
		indexs = append(indexs, rf.matchIndex[i])
	}
	sort.Ints(indexs)
	index := len(indexs) / 2
	commit := indexs[index]
	if commit > rf.commitIndex {
		log.Println(rf.me, "update leader commit index", commit)
		rst = true
		rf.commitIndex = commit
	}
	return rst
}

//apply 状态机
func (rf *Raft) apply()  {
	if rf.status == Leader {
		rf.updateCommitIndex()
	}
	_, last := rf.getLogTermAndIndex()
	for ; rf.lastApplied < rf.commitIndex && rf.lastApplied < last; rf.lastApplied++ {
		index := rf.lastApplied
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[index].Log,
			CommandIndex: rf.logs[index].Index,
		}
		log.Println(rf.me, "apply log", index, rf.logs[index].Term,"-", rf.logs[index].Index)
		log.Println(rf.me, "apply logint", rf.logs[index].Log.(int))
		rf.applyCh <- msg
	}
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
	//插入log
	rf.logs = append(rf.logs, entry)
	return index
}

func (rf *Raft) RequestAppendEntries(req *AppendEntries, resp *RespEntries) {
	rf.lock("Raft.RequestAppendEntries")
	defer rf.unlock("Raft.RequestAppendEntries")
	resp.Term = rf.currentTerm
	resp.Successed = true
	if req.Term < rf.currentTerm {
		//leader任期小于自身任期，则拒绝同步log
		resp.Successed = false
		return
	}
	//否则更新自身任期，切换自生为fallow，充值选举定时器
	rf.resetCandidateTimer()
	rf.currentTerm = req.Term
	rf.setStatus(Fallower)
	//判定与leader日志是一致
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > len(rf.logs) {
			//没有该日志，则拒绝更新
			log.Println(rf.me, "can't find preindex", req.PrevLogTerm)
			resp.Successed = false
			return
		}
		if rf.logs[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			//该索引与自身日志不同，则拒绝更新
			log.Println(rf.me, "term error", req.PrevLogTerm)
			resp.Successed = false
			return
		}
	}
	//更新日志
	size := len(req.Entries)
	if size > 0 {
		log.Println(rf.me, "update log from ", req.Me)
	}
	for i := 0; i < size; i++ {
		rf.updateLog(req.PrevLogIndex+i, req.Entries[i])
	}
	/*
		//删除不同步索引
		for i := index + size + 1; i < len(rf.logs); i++ {
			key := rf.logs[i].Index
			delete(rf.logIndexs, key)
		}
		//删除不同步日志
		rf.logs = rf.logs[:(index + size + 1)]
	*/
	rf.commitIndex = req.LeaderCommit
	rf.apply()
	return
}

func (rf *Raft) replicateLogTo(peer int) bool {
	replicateRst := false
	if peer == rf.me {
		return replicateRst
	}
	isLoop := true
	for rf.status == Leader && isLoop && !rf.isKilled {
		isLoop = false
		rf.lock("Raft.RequestAppendEntries")
		req := AppendEntries{
			Me:           rf.me,
			Term:         rf.currentTerm,
			LeaderCommit: rf.commitIndex,
		}
		resp := RespEntries{Term: 0}
		//当前fallow的日志状态
		next := rf.nextIndex[peer]
		req.PrevLogTerm, req.PrevLogIndex = rf.getEntriesInfo(next, &req.Entries)
		if len(req.Entries) > 0 {
			log.Println(rf.me, "replicate log to ", peer, " preterm", req.PrevLogTerm, " preindex", req.PrevLogIndex)
		}
		rf.unlock("Raft.RequestAppendEntries")
		rst := rf.sendAppendEnteries(peer, &req, &resp)
		rf.lock("Raft.RequestAppendEntries")
		if rst {
			//如果某个节点任期大于自己，则更新任期，变成fallow
			if resp.Term > rf.currentTerm {
				log.Println(rf.me, "become fallow ", peer, "term :", resp.Term)
				rf.setStatus(Fallower)
			} else if !resp.Successed { //如果更新失败则fallow日志状态减1
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer]--
					log.Println(rf.me, "to", peer, "replicate log error", rf.nextIndex[peer])
					isLoop = true
				}
			} else { //更新成功
				if len(req.Entries) > 0 {
					rf.nextIndex[peer] = req.Entries[len(req.Entries)-1].Index + 1
					rf.matchIndex[peer] = req.Entries[len(req.Entries)-1].Index
					replicateRst = true
				}
			}
		} else {
			isLoop = true
		}
		rf.unlock("Raft.RequestAppendEntries")
	}
	return replicateRst
}

func (rf *Raft) replicateLogNow() {
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i].Reset(0)
	}
}

func (rf *Raft) ReplicateLogLoop(peer int) {
	defer func() {
		rf.heartbeatTimers[peer].Stop()
	}()
	for !rf.isKilled {
		<-rf.heartbeatTimers[peer].C
		if rf.isKilled {
			break
		}
		if rf.status == Leader {
			success := rf.replicateLogTo(peer)
			if success {
				rf.lock("Raft.ReplicateLogLoop")
				rf.apply()
				rf.unlock("Raft.ReplicateLogLoop")
				rf.replicateLogNow()
			}
		}
		rf.heartbeatTimers[peer].Reset(HeartbeatDuration)
	}
}

func (rf *Raft) Start(command interface{}) (index int,term int, isLeader bool) {
	rf.lock("Raft.Start")
	defer rf.unlock("Raft.Start")
	term = 0
	index = 0
	isLeader = rf.status == Leader

	if isLeader {
		//设置term并插入log
		term = rf.currentTerm
		index = rf.insertLog(command)
		log.Println("leader", rf.me, ":", "append log", term, "-", index)
		rf.replicateLogNow()
	}
	return
}

func (rf *Raft) Kill() {
	rf.isKilled = true
	rf.eletionTimer.Reset(0)
	rf.replicateLogNow()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.randtime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.isKilled = false
	rf.heartbeatTimers = make([]*time.Timer, len(rf.peers))
	rf.eletionTimer = time.NewTimer(CandidateDuration)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.setStatus(Fallower)

	//日志同步协程
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i] = time.NewTimer(HeartbeatDuration)
		go rf.ReplicateLogLoop(i)
	}
	rf.readPersist(persister.ReadRaftState())
	raftOnce.Do(func() {
		log.SetFlags(log.Ltime | log.Lmicroseconds | log.Lshortfile)
	})
	//Leader选举协程
	go rf.ElectionLoop()

	return rf
}
