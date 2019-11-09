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

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	index int
}
type TermLog struct {
	TermIndex int
	logs      []Log
}

//节点状态
const Fallower, Leader, Candidate int = 1, 2, 3

//心跳超时阈值
const HeartbeatTimeoutCnt int64 = 3

//心跳周期
const HeartbeatDuration = time.Duration(time.Millisecond * 400)

//竞选周期随机范围，毫秒。
const CandidateDuration int = 300

//投票请求
type RequestVoteArgs struct {
	Me              int
	ElectionTerm    int64
	CurrentLogIndex int
	CurrentLogTerm  int
}

//投票rpc返回
type RequestVoteReply struct {
	IsAgree     bool
	CurrentTerm int64
}

type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // rpc节点
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // 自己服务编号
	logs           []TermLog           // 日志存储
	heartBeatCnt   int64               // 心跳计数
	status         int                 //节点状态
	currentTerm    int64               //当前周期
	heartbeatTimer *time.Timer         //心跳定时器
	candidateTimer *time.Timer         //竞选超时定时器
	randtime       *rand.Rand          //随机数，用于随机竞选周期，避免节点间竞争。
	killChan       chan (int)          //退出节点
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	term = len(rf.logs)
	isleader = rf.status == Leader
	return term, isleader
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//

type RequestHeartbeat struct {
	Me   int
	Term int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RespHeartbeat struct {

	// Your data here (2A).
}

func (rf *Raft) setStatus(status int) {
	//设置节点状态，变换为fallow时候重置选举定时器
	if (rf.status != Fallower) && (status == Fallower) {
		rf.resetCandidateTimer()
	}
	rf.status = status
}

func (rf *Raft) getLogIndex() int {
	termlogs := rf.logs[len(rf.logs)-1].logs
	logindex := termlogs[len(termlogs)-1].index
	return logindex
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	fmt.Println(rf.me, "vote for", args.Me)
	reply.IsAgree = true
	reply.CurrentTerm = rf.currentTerm
	//竞选任期小于自身任期，则反对票
	if rf.currentTerm >= args.ElectionTerm {
		reply.IsAgree = false
		fmt.Println(rf.me, "vote for", args.Me, "false")
		return
	}
	//竞选任期大于自身任期，则更像自身任期，并转为fallow
	rf.setStatus(Fallower)
	rf.currentTerm = args.ElectionTerm
	if reply.IsAgree {
		//赞同票后重置心跳（避免竞选竞争，增加一次选举成功的概率）
		rf.heartBeatCnt = 0
	}
	/*
		if args.CurrentLogIndex < rf.getLogIndex() {
			reply.IsAgree = false
			return
		}
	*/
	// Your code here (2A, 2B).
}

func (rf *Raft) OnHeartbeat(req *RequestHeartbeat, resp *RespHeartbeat) {
	if req.Term > rf.currentTerm {
		//发现其他节点任期大于自身任期，则变为fallow，并更新任期
		fmt.Println(rf.me, "become fallow")
		rf.currentTerm = req.Term
		rf.setStatus(Fallower)
		atomic.StoreInt64(&rf.heartBeatCnt, 0)
	} else if req.Term == rf.currentTerm {
		if rf.status == Fallower {
			atomic.StoreInt64(&rf.heartBeatCnt, 0)
		}
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeat(server int, req *RequestHeartbeat, resp *RespHeartbeat) bool {
	ok := rf.peers[server].Call("Raft.OnHeartbeat", req, resp)
	return ok
}

//重置竞选周期定时
func (rf *Raft) resetCandidateTimer() {
	randCnt := rf.randtime.Intn(CandidateDuration)
	cnt := time.Duration(randCnt)*time.Millisecond + time.Duration(HeartbeatTimeoutCnt)*HeartbeatDuration
	duration := time.Duration(cnt)
	rf.candidateTimer.Reset(duration)
}


func (rf *Raft) Vote() {
	//投票先增大自身任期
	rf.currentTerm++
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), "try vote :", rf.me, "term :", rf.currentTerm)
	req := RequestVoteArgs{
		Me:           rf.me,
		ElectionTerm: rf.currentTerm,
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
			rstChan := make(chan (bool))
			rst := false
			go func() {
				rst := rf.sendRequestVote(index, &req, &resp)
				rstChan <- rst
			}()
			select {
			case rst = <-rstChan:
			case <-time.After(time.Second * 1):
				//rpc调用超时
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), "rpc timeout")
			}
			if !rst {
				fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), "rpc error")
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
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), "after vote :", rf.me)
	//如果存在系统任期更大，则更像任期并转为fallow
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.setStatus(Fallower)
		return
	}
	//获得多数赞同则变成leader
	fmt.Println(rf.me, "get agree cnt", agreeVote)
	if agreeVote*2 > peercnt {
		fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), rf.me, "become leader.")
		rf.setStatus(Leader)
		req := RequestHeartbeat{
			Me:   rf.me,
			Term: rf.currentTerm,
		}
		resp := RespHeartbeat{}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(index int) {
					rf.sendHeartbeat(index, &req, &resp)
				}(i)
			}
		}
	}
}

//心跳周期
func (rf *Raft) onHeartbeatTimerout() {
	if rf.status == Fallower {
		//心跳技术+1
		atomic.AddInt64(&rf.heartBeatCnt, 1)
	} else if rf.status == Leader {
		req := RequestHeartbeat{
			Me:   rf.me,
			Term: rf.currentTerm,
		}
		resp := RespHeartbeat{}
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(index int) {
					rf.sendHeartbeat(index, &req, &resp)
				}(i)
			}
		}
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
			fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), rf.me, "become candidate", rf.heartBeatCnt)
			rf.setStatus(Candidate)
			rf.Vote()
		}
		rf.resetCandidateTimer()
	}
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
		case <-rf.killChan:
			return
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	fmt.Println(rf.me, "start.")
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	fmt.Println(time.Now().Format("2006-01-02 15:04:05.000"), rf.me, "kill.")
	close(rf.killChan)
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
	rf.randtime = rand.New(rand.NewSource(time.Now().UnixNano()))
	rf.killChan = make(chan (int))
	//主循环
	go rf.MainLoop()
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
