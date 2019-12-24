package raft

import "sync"
import "labrpc"
import "time"
import "math/rand"
import "log"
import "bytes"
import "labgob"
import "sort"


//import "os"
//节点状态
const Fallower, Leader, Candidate int = 1, 2, 3

//心跳周期
const HeartbeatDuration = time.Duration(time.Millisecond * 600)

//竞选周期
const CandidateDuration = HeartbeatDuration * 2

var raftOnce sync.Once

//状态机apply
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
//日志快照
type LogSnapshot struct {
	Term  int
	Index int
	Datas []byte
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
	Snapshot     LogSnapshot //快照
}

//回复日志更新请求
type RespEntries struct {
	Term        int
	Successed   bool
	LastApplied int
}

type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // rpc节点
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // 自己服务编号
	logs            []LogEntry          // 日志存储
	logSnapshot     LogSnapshot         //日志快照
	commitIndex     int                 //当前日志提交处
	lastApplied     int                 //当前状态机执行处
	status          int                 //节点状态
	currentTerm     int                 //当前任期
	heartbeatTimers []*time.Timer       //心跳定时器
	eletionTimer    *time.Timer         //竞选超时定时器
	randtime        *rand.Rand          //随机数，用于随机竞选周期，避免节点间竞争。

	nextIndex      []int         //记录每个fallow的同步日志状态
	matchIndex     []int         //记录每个fallow日志最大索引，0递增
	applyCh        chan ApplyMsg //状态机apply
	isKilled       bool          //节点退出
	lastLogs       AppendEntries //最后更新日志
	EnableDebugLog bool          //打印调试日志开关
	LastGetLock    string
}

//打印调试日志
func (rf *Raft) println(args ...interface{}) {
	if rf.EnableDebugLog {
		log.Println(args...)
	}
}

func (rf *Raft) lock(info string) {
	rf.mu.Lock()
	rf.LastGetLock = info
}

func (rf *Raft) unlock(info string) {
	rf.LastGetLock = ""
	rf.mu.Unlock()
}

//投票RPC
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rstChan := make(chan (bool))
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestVote", args, reply)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(HeartbeatDuration):
		//rpc调用超时
	}
	return ok
}

//同步日志RPC
func (rf *Raft) sendAppendEnteries(server int, req *AppendEntries, resp *RespEntries) bool {
	rstChan := make(chan (bool))
	ok := false
	go func() {
		rst := rf.peers[server].Call("Raft.RequestAppendEntries", req, resp)
		rstChan <- rst
	}()
	select {
	case ok = <-rstChan:
	case <-time.After(time.Millisecond * 400):
	}
	return ok
}

//获取状态和任期
func (rf *Raft) GetState() (int, bool) {
	rf.lock("Raft.GetState")
	defer rf.unlock("Raft.GetState")
	isleader := rf.status == Leader
	return rf.currentTerm, isleader
}

//设置任期
func (rf *Raft) setTerm(term int) {
	rf.lock("Raft.setTerm")
	defer rf.unlock("Raft.setTerm")
	rf.currentTerm = term
}

//增加任期
func (rf *Raft) addTerm(term int) {
	rf.lock("Raft.addTerm")
	defer rf.unlock("Raft.addTerm")
	rf.currentTerm += term
}

//设置当前节点状态
func (rf *Raft) setStatus(status int) {
	rf.lock("Raft.setStatus")
	defer rf.unlock("Raft.setStatus")
	//设置节点状态，变换为fallow时候重置选举定时器（避免竞争）
	if (rf.status != Fallower) && (status == Fallower) {
		rf.resetCandidateTimer()
	}

	//节点变为leader，则初始化fallow日志状态
	if rf.status != Leader && status == Leader {
		index := len(rf.logs)
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = index + 1 + rf.logSnapshot.Index
			rf.matchIndex[i] = 0
		}
	}
	rf.status = status
}

//获取状态
func (rf *Raft) getStatus() int {
	rf.lock("Raft.getStatus")
	defer rf.unlock("Raft.getStatus")
	return rf.status
}

//获取提交日志索引
func (rf *Raft) getCommitIndex() int {
	rf.lock("Raft.getCommitedCnt")
	defer rf.unlock("Raft.getCommitedCnt")
	return rf.commitIndex
}

//设置提交日志索引
func (rf *Raft) setCommitIndex(index int) {
	rf.lock("Raft.setCommitIndex")
	defer rf.unlock("Raft.setCommitIndex")
	rf.commitIndex = index
}

//获取日志索引及任期
func (rf *Raft) getLogTermAndIndex() (int, int) {
	rf.lock("Raft.getLogTermAndIndex")
	defer rf.unlock("Raft.getLogTermAndIndex")
	index := 0
	term := 0
	size := len(rf.logs)
	if size > 0 {
		index = rf.logs[size-1].Index
		term = rf.logs[size-1].Term
	} else {
		index = rf.logSnapshot.Index
		term = rf.logSnapshot.Term
	}
	return term, index
}

//获取索引处及任期
func (rf *Raft) getLogTermOfIndex(index int) int {
	rf.lock("Raft.getLogTermOfIndex")
	defer rf.unlock("Raft.getLogTermOfIndex")
	index -=(1+rf.logSnapshot.Index)
	if index < 0 {
		return rf.logSnapshot.Term
	}
	return rf.logs[index].Term
}

//获取快照
func (rf *Raft) getSnapshot(index int, snapshot *LogSnapshot) int {
	if index <= rf.logSnapshot.Index { //如果fallow日志小于快照，则获取快照
		*snapshot = rf.logSnapshot
		index = 0 //更新快照时，从0开始复制日志
	} else {
		index -= rf.logSnapshot.Index
	}
	return index
}

//获取该节点更新日志
func (rf *Raft) getEntriesInfo(index int, snapshot *LogSnapshot,entries *[]LogEntry) (preterm int, preindex int) {
	start := rf.getSnapshot( index , snapshot)-1
	if start < 0 {
		preindex = 0
		preterm = 0
	} else if start == 0 {
		if rf.logSnapshot.Index ==0 {
			preindex = 0
			preterm = 0
		}  else {
			preindex = rf.logSnapshot.Index
			preterm = rf.logSnapshot.Term
		}
	} else {
		preindex = rf.logs[start-1].Index
		preterm = rf.logs[start-1].Term
	}
	if start < 0 {
		start = 0
	}
	for i := start; i < len(rf.logs); i++ {
		*entries = append(*entries, rf.logs[i])
	}
	return
}

//获取该节点更新日志及信息
func (rf *Raft) getAppendEntries(peer int) AppendEntries {
	rf.lock("Raft.getAppendEntries")
	defer rf.unlock("Raft.getAppendEntries")
	rst := AppendEntries{
		Me:           rf.me,
		Term:         rf.currentTerm,
		LeaderCommit: rf.commitIndex ,
		Snapshot : LogSnapshot{Index:0},
	}
	//当前fallow的日志状态
	next := rf.nextIndex[peer]
	rst.PrevLogTerm, rst.PrevLogIndex = rf.getEntriesInfo(next, &rst.Snapshot, &rst.Entries)
	return rst
}

//减少fallow next日志索引
func (rf *Raft) incNext(peer int) {
	rf.lock("Raft.incNext")
	defer rf.unlock("Raft.incNext")
	if rf.nextIndex[peer] > 1 {
		rf.nextIndex[peer]--
	}
}

//设置fallow next日志索引
func (rf *Raft) setNext(peer int, next int) {
	rf.lock("Raft.setNext")
	defer rf.unlock("Raft.setNext")
	rf.nextIndex[peer] = next
}

//设置fallower next和match日志索引
func (rf *Raft) setNextAndMatch(peer int, index int) {
	rf.lock("Raft.setNextAndMatch")
	defer rf.unlock("Raft.setNextAndMatch")
	rf.nextIndex[peer] = index + 1
	rf.matchIndex[peer] = index
}

//更新插入同步日志
func (rf *Raft) updateLog(start int, logEntrys []LogEntry,snapshot *LogSnapshot) {
	rf.lock("Raft.updateLog")
	defer rf.unlock("Raft.updateLog")
	if snapshot.Index > 0 {   //更新快照
		rf.logSnapshot = *snapshot
		start = rf.logSnapshot.Index
		rf.println("update snapshot :",rf.me,rf.logSnapshot.Index,"len logs",len(logEntrys))
	}
	index := start - rf.logSnapshot.Index
	for i := 0; i < len(logEntrys); i++ {
		if index+i <0 {
			//网络不可靠，fallower节点成功apply并保存快照后，Leader未收到反馈，重复发送日志，
			//可能会导致index <0情况。
			continue
		}
		if index+i < len(rf.logs) {
			rf.logs[index+i] = logEntrys[i]
		} else {
			rf.logs = append(rf.logs, logEntrys[i])
		}
	}
	size  := index+len(logEntrys)
	if size <0 { //网络不可靠+各节点独立备份快照可能出现
		size =0
	}
	//重置log大小
	rf.logs = rf.logs[:size]
}

//插入日志
func (rf *Raft) insertLog(command interface{}) int {
	rf.lock("Raft.insertLog")
	defer rf.unlock("Raft.insertLog")
	entry := LogEntry{
		Term:  rf.currentTerm,
		Index: 1,
		Log:   command,
	}
	//获取log索引
	if len(rf.logs) > 0 {
		entry.Index = rf.logs[len(rf.logs)-1].Index + 1
	} else {
		entry.Index = rf.logSnapshot.Index+1
	}
	//插入log
	rf.logs = append(rf.logs, entry)
	return entry.Index
}

//获取当前已被提交日志
func (rf *Raft) updateCommitIndex() bool {
	rst := false
	var indexs []int
	rf.matchIndex[rf.me] = 0
	if len(rf.logs) > 0 {
		rf.matchIndex[rf.me] = rf.logs[len(rf.logs)-1].Index
	} else {
		rf.matchIndex[rf.me] = rf.logSnapshot.Index
	}
	for i := 0; i < len(rf.matchIndex); i++ {
		indexs = append(indexs, rf.matchIndex[i])
	}
	sort.Ints(indexs)
	index := len(indexs) / 2
	commit := indexs[index]
	if commit > rf.commitIndex {
		rf.println(rf.me, "update leader commit index", commit)
		rst = true
		rf.commitIndex = commit
	}
	return rst
}

//apply 状态机
func (rf *Raft) apply() {
	rf.lock("Raft.apply")
	defer rf.unlock("Raft.apply")
	if rf.status == Leader {
		rf.updateCommitIndex()
	}

	lastapplied := rf.lastApplied
	if rf.lastApplied< rf.logSnapshot.Index {
		msg := ApplyMsg{
			CommandValid: false,
			Command:      rf.logSnapshot,
			CommandIndex: 0,
		}
		rf.applyCh <- msg
		rf.lastApplied = rf.logSnapshot.Index
		rf.println(rf.me,"apply snapshot :", rf.logSnapshot.Index,"with logs:",len(rf.logs))
	}
	last := 0
	if len(rf.logs) > 0 {
		last = rf.logs[len(rf.logs)-1].Index
	}
	for ; rf.lastApplied < rf.commitIndex && rf.lastApplied < last; rf.lastApplied++ {
		index := rf.lastApplied
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[index-rf.logSnapshot.Index].Log,
			CommandIndex: rf.logs[index-rf.logSnapshot.Index].Index,
		}
		rf.applyCh <- msg
	}
	if rf.lastApplied > lastapplied {	
		appliedIndex := rf.lastApplied-1-rf.logSnapshot.Index
		endIndex,endTerm :=0,0
		if appliedIndex < 0 {
			endIndex = rf.logSnapshot.Index
			endTerm = rf.logSnapshot.Term
		} else {
			endTerm = rf.logs[rf.lastApplied-1-rf.logSnapshot.Index].Term
			endIndex = rf.logs[rf.lastApplied-1-rf.logSnapshot.Index].Index
		}
		rf.println(rf.me, "apply log", rf.lastApplied-1,endTerm, "-", endIndex,"/",last)
	}
}

//设置最后一次提交（用于乱序判定）
func (rf *Raft) setLastLog(req *AppendEntries) {
	rf.lock("Raft.setLastLog")
	defer rf.unlock("Raft.setLastLog")
	rf.lastLogs = *req
}

//判定乱序
func (rf *Raft) isOldRequest(req *AppendEntries) bool {
	rf.lock("Raft.isOldRequest")
	defer rf.unlock("Raft.isOldRequest")
	if req.Term == rf.lastLogs.Term && req.Me == rf.lastLogs.Me {
		lastIndex := rf.lastLogs.PrevLogIndex + rf.lastLogs.Snapshot.Index+len(rf.lastLogs.Entries)
		reqLastIndex := req.PrevLogIndex + req.Snapshot.Index+ len(req.Entries)
		return lastIndex > reqLastIndex
	}
	return false
}

//重置竞选周期定时
func (rf *Raft) resetCandidateTimer() {
	randCnt := rf.randtime.Intn(250)
	duration := time.Duration(randCnt)*time.Millisecond + CandidateDuration
	rf.eletionTimer.Reset(duration)
}

func (rf *Raft) persist() {
	rf.lock("Raft.persist")
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.commitIndex)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.lastLogs)
	encoder.Encode(rf.logSnapshot.Index)
    encoder.Encode(rf.logSnapshot.Term)
	data := writer.Bytes()
	//rf.persister.SaveRaftState(data)
	rf.unlock("Raft.persist")
	rf.persister.SaveStateAndSnapshot(data,rf.logSnapshot.Datas)
	//持久化数据后，apply 通知触发快照
	msg := ApplyMsg{
		CommandValid: false,
		Command:     nil,
		CommandIndex: 0,
	}
	rf.applyCh <- msg
}

func (rf *Raft) readPersist(data []byte) {
	rf.lock("Raft.readPersist")
	if data == nil || len(data) < 1 {
		rf.unlock("Raft.readPersist")
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var commitIndex, currentTerm int
	var logs []LogEntry
	var lastlogs AppendEntries
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&commitIndex) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&lastlogs) != nil  ||
		decoder.Decode(&rf.logSnapshot.Index) != nil ||
        decoder.Decode(&rf.logSnapshot.Term) != nil {
		rf.println("Error in unmarshal raft state")
	} else {

		rf.currentTerm = currentTerm
		rf.commitIndex = commitIndex
		rf.lastApplied = 0
		rf.logs = logs
		rf.lastLogs = lastlogs
	}
	rf.unlock("Raft.readPersist")
	rf.logSnapshot.Datas = rf.persister.ReadSnapshot()
}

//收到投票请求
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	reply.IsAgree = true
	reply.CurrentTerm, _ = rf.GetState()
	//竞选任期小于自身任期，则反对票
	if reply.CurrentTerm >= req.ElectionTerm {
		rf.println(rf.me, "refuse", req.Me, "because of term")
		reply.IsAgree = false
		return
	}
	//竞选任期大于自身任期，则更新自身任期，并转为fallow
	rf.setStatus(Fallower)
	rf.setTerm(req.ElectionTerm)
	logterm, logindex := rf.getLogTermAndIndex()
	//判定竞选者日志是否新于自己
	if logterm > req.LogTerm {
		rf.println(rf.me, "refuse", req.Me, "because of logs's term")
		reply.IsAgree = false
	} else if logterm == req.LogTerm {
		reply.IsAgree = logindex <= req.LogIndex
		if !reply.IsAgree {
			rf.println(rf.me, "refuse", req.Me, "because of logs's index")
		}
	}
	if reply.IsAgree {
		rf.println(rf.me, "agree", req.Me)
		//赞同票后重置选举定时，避免竞争
		rf.resetCandidateTimer()
	}
}

func (rf *Raft) Vote() {
	//投票先增大自身任期
	rf.addTerm(1)
	rf.println("start vote :", rf.me, "term :", rf.currentTerm)
	logterm, logindex := rf.getLogTermAndIndex()
	currentTerm, _ := rf.GetState()
	req := RequestVoteArgs{
		Me:           rf.me,
		ElectionTerm: currentTerm,
		LogTerm:      logterm,
		LogIndex:     logindex,
	}
	var wait sync.WaitGroup
	peercnt := len(rf.peers)
	wait.Add(peercnt)
	agreeVote := 0
	term := currentTerm
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
	//如果存在系统任期更大，则更新任期并转为fallow
	if term > currentTerm {
		rf.setTerm(term)
		rf.setStatus(Fallower)
	} else if agreeVote*2 > peercnt { //获得多数赞同则变成leader
		rf.println(rf.me, "become leader :", currentTerm)
		rf.setStatus(Leader)
		rf.replicateLogNow()
	}
}

//选举定时器loop
func (rf *Raft) ElectionLoop() {
	//选举超时定时器
	rf.resetCandidateTimer()
	defer rf.eletionTimer.Stop()

	for !rf.isKilled {
		<-rf.eletionTimer.C
		if rf.isKilled {
			break
		}
		if rf.getStatus() == Candidate {
			//如果状态为竞选者，则直接发动投票
			rf.resetCandidateTimer()
			rf.Vote()
		} else if rf.getStatus() == Fallower {
			//如果状态为fallow，则转变为candidata并发动投票
			rf.setStatus(Candidate)
			rf.resetCandidateTimer()
			rf.Vote()
		}
	}
	rf.println(rf.me,"Exit ElectionLoop")
}

func (rf *Raft) RequestAppendEntries(req *AppendEntries, resp *RespEntries) {
	currentTerm, _ := rf.GetState()
	resp.Term = currentTerm
	resp.Successed = true
	if req.Term < currentTerm {
		//leader任期小于自身任期，则拒绝同步log
		resp.Successed = false
		return
	}
	//乱序日志，不处理
	if rf.isOldRequest(req) {
		return
	}
	//否则更新自身任期，切换自生为fallow，重置选举定时器
	rf.resetCandidateTimer()
	rf.setTerm(req.Term)
	rf.setStatus(Fallower)
	_, logindex := rf.getLogTermAndIndex()
	//判定与leader日志是一致
	if req.PrevLogIndex > 0 {
		if req.PrevLogIndex > logindex {
			//没有该日志，则拒绝更新
			//rf.println(rf.me, "can't find preindex", req.PrevLogTerm)
			resp.Successed = false
			resp.LastApplied = rf.lastApplied
			return
		}
		if rf.getLogTermOfIndex(req.PrevLogIndex) != req.PrevLogTerm {
			//该索引与自身日志不同，则拒绝更新
			//rf.println(rf.me, "term error", req.PrevLogTerm)
			resp.Successed = false
			resp.LastApplied = rf.lastApplied
			return
		}
	}
	//更新日志
	rf.setLastLog(req)
	if  len(req.Entries) > 0 || req.Snapshot.Index > 0 {
		if len(req.Entries) > 0{
			rf.println(rf.me, "update log from ", req.Me, ":", req.Entries[0].Term, "-", req.Entries[0].Index, "to", req.Entries[len(req.Entries)-1].Term, "-", req.Entries[len(req.Entries)-1].Index)
		}
		rf.updateLog(req.PrevLogIndex, req.Entries,&req.Snapshot)
	}
	rf.setCommitIndex(req.LeaderCommit)
	rf.apply()
	rf.persist()

	return
}

//复制日志给fallower
func (rf *Raft) replicateLogTo(peer int) bool {
	replicateRst := false
	if peer == rf.me {
		return replicateRst
	}
	isLoop := true
	for isLoop {
		isLoop = false
		currentTerm, isLeader := rf.GetState()
		if !isLeader || rf.isKilled {
			break
		}
		req := rf.getAppendEntries(peer)
		resp := RespEntries{Term: 0}
		rst := rf.sendAppendEnteries(peer, &req, &resp)
		currentTerm, isLeader = rf.GetState()
		if rst && isLeader {
			//如果某个节点任期大于自己，则更新任期，变成fallow
			if resp.Term > currentTerm {
				rf.println(rf.me, "become fallow ", peer, "term :", resp.Term)
				rf.setTerm(resp.Term)
				rf.setStatus(Fallower)
			} else if !resp.Successed { //如果更新失败则更新fallow日志next索引
				//rf.incNext(peer)
				rf.setNext(peer, resp.LastApplied+1)
				isLoop = true
			} else { //更新成功
				if len(req.Entries) > 0 {
					rf.setNextAndMatch(peer, req.Entries[len(req.Entries)-1].Index)
					replicateRst = true
				} else if req.Snapshot.Index >0 {
					rf.setNextAndMatch(peer, req.Snapshot.Index)
					replicateRst = true
				}
			}
		} else {
			isLoop = true
		}
	}
	return replicateRst
}

//立即复制日志
func (rf *Raft) replicateLogNow() {
	rf.lock("Raft.replicateLogNow")
	defer rf.unlock("Raft.replicateLogNow")
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i].Reset(0)
	}
}

//心跳周期复制日志loop
func (rf *Raft) ReplicateLogLoop(peer int) {
	defer func() {
		rf.heartbeatTimers[peer].Stop()
	}()
	for !rf.isKilled {
		<-rf.heartbeatTimers[peer].C
		if rf.isKilled {
			break
		}
		rf.lock("Raft.ReplicateLogLoop")
		rf.heartbeatTimers[peer].Reset(HeartbeatDuration)
		rf.unlock("Raft.ReplicateLogLoop")
		_, isLeader := rf.GetState()
		if isLeader {
			success := rf.replicateLogTo(peer)
			if success {
				rf.apply()
				rf.replicateLogNow()
				rf.persist()
			}
		}
	}
	rf.println(rf.me,"-",peer,"Exit ReplicateLogLoop")
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	index = 0
	term, isLeader = rf.GetState()
	if isLeader {
		//设置term并插入log
		index = rf.insertLog(command)
		rf.println("leader", rf.me, ":", "append log", term, "-", index)
		rf.replicateLogNow()
	}
	return
}

func (rf *Raft) Kill() {
	rf.isKilled = true
	rf.eletionTimer.Reset(0)
	rf.replicateLogNow()
}


//保存快照
func (rf *Raft) SaveSnapshot(index int,snapshot []byte) {
	rf.lock("Raft.SaveSnapshot")
	if index > rf.logSnapshot.Index {
		//保存快照
		start := rf.logSnapshot.Index
		rf.logSnapshot.Index = index
		rf.logSnapshot.Datas = snapshot
		rf.logSnapshot.Term = rf.logs[index-start-1].Term
		//删除快照日志
		if len(rf.logs) >0 {
			rf.logs = rf.logs[(index-start):]
		}
		rf.println("save snapshot :",rf.me,index,",len logs:",len(rf.logs))
		rf.unlock("Raft.SaveSnapshot")
		rf.persist()
	} else {
		rf.unlock("Raft.SaveSnapshot")
	}
	
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
	rf.EnableDebugLog = false
	rf.lastLogs = AppendEntries{
		Me:   -1,
		Term: -1,
	}
	rf.logSnapshot = LogSnapshot{
		Index : 0,
		Term :0 ,
	}
	//日志同步协程
	for i := 0; i < len(rf.peers); i++ {
		rf.heartbeatTimers[i] = time.NewTimer(HeartbeatDuration)
		go rf.ReplicateLogLoop(i)
	}
	rf.readPersist(persister.ReadRaftState())
	rf.apply()
	raftOnce.Do(func() {
		//filename :=  "log"+time.Now().Format("2006-01-02 15_04_05") +".txt"
		//file, _ := os.OpenFile(filename, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		//log.SetOutput(file)
		log.SetFlags(log.Ltime | log.Lmicroseconds)
	})
	//Leader选举协程
	go rf.ElectionLoop()

	return rf
}
