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
import "math/rand"
import "time"

// import "bytes"
// import "encoding/gob"

const (
	NONE      = -1
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Term    int
	Index   int
	Content interface{}
}

type AppendEntriesArgs struct {
	Term         int //leader的term
	LeaderId     int //leader的标识
	PrevLogIndex int //之前log的Index
	PrevLogTerm  int //之前log的term
	Entries      []LogEntry
	LeaderCommit int //leader的commitIndex
	Me           int //标识请求的发起者
}

type AppendEntriesReply struct {
	Term int //返回的term，用于leader更新自己
	Succ bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	voteLock  sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTimeoutMS          int   //选举超时时间，时间单位为毫秒
	nodeRole                   int   //当前节点的角色状态，默认为follower
	currentTerm                int   //当前的term
	commitIndex                int   //当前已提交的index
	lastLeaderHeartBeatTime    int64 //上次心跳时，unix时间戳，单位是毫秒
	raftHeartBeatIntervalMilli int   //raft心跳间隔，单位是毫秒，任务初始化时指定
	entries                    []LogEntry
	raftIsShutdown             bool //当前进程是否关闭
	votedFor                   int  //当前term投票给谁了
	leaderHeartCheckerSwitch   bool //用于检测在leader心跳的开关
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	if rf.getRaftRole() == LEADER {
		isleader = true
	}
	term = rf.getRaftTerm()
	return term, isleader
}

func (rf *Raft) setRaftRole(role int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nodeRole = role
}

func (rf *Raft) getRaftRole() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nodeRole
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选者当前的term
	CandidateId  int //候选者的term
	LastLogIndex int //候选者最新log的index
	LastLogTerm  int //候选者最新log的term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //用于候选者更新自己当前的term
	VoteGranted bool //投票的结果，如果成功了，那么就返回true
}

/*
example RequestVote RPC handler.
选举操作只判断状态，不更新状态。状态的更新应该是在leader选举出来之后
该方法的调用者只能是候选者，而接收者可以使任何人

1,如果是leader接收到了该请求，如果发现更高的term，那么就退回follower
2,如果是候选者接收到了，那么和leader做同样的处理

该方法的接收者必须满足
1，请求者的log以及term比自己新，才能成为leader
2，只能投一个
*/
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.voteLock.Lock()
	defer rf.voteLock.Unlock()
	reply.Term = rf.getRaftTerm()
	tmpVotedFor := rf.getVotedFor()
	rf.setLastLeaderHeartBeatTime()
	if rf.getRaftTerm() > req.Term {
		DPrintf("term=%d,role=%s,rf=%d 拒绝rf=%d的投票请求，因为req的term比较小,reqTerm=%d", rf.getRaftTerm(),
			getRole(rf.getRaftRole()), rf.me, req.CandidateId, req.Term)
		return
	} else if rf.getRaftTerm() < req.Term {
		rf.setRaftTerm(req.Term)
		rf.becomeFollower(NONE)
		rf.setElectionTimeOut()
	}
	//当前的raft是有主的，那么必然要拒绝其他选举请求
	if rf.getVotedFor() >= 0 {
		DPrintf("term=%d,role=%s,rf=%d 投过了票所以拒绝了rf=%d的选举请求,votedFor=%d,reqTerm=%d", rf.getRaftTerm(),
			getRole(rf.getRaftRole()), rf.me, req.CandidateId, rf.getVotedFor(), req.Term)
		reply.VoteGranted = false
		return
	}

	lastEntry := rf.entries[len(rf.entries)-1]
	if lastEntry.Term > req.LastLogTerm {
		reply.VoteGranted = false
		return
	} else if lastEntry.Term == req.LastLogTerm {
		if rf.commitIndex <= req.LastLogIndex {
			reply.VoteGranted = true
			rf.setRaftRole(FOLLOWER)
			rf.setVotedFor(req.CandidateId)
			rf.setElectionTimeOut()
			DPrintf("term=%d,role=%s,votedFor=%d,rf=%d同意了rf=%d的选举请求,之前的voteFor为%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.getVotedFor(), rf.me, req.CandidateId, tmpVotedFor)
		} else {
			reply.VoteGranted = false
			DPrintf("term=%d,role=%s,votedFor=%d,rf=%d拒绝了rf=%d的选举请求,req的日志长度比较小", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.getVotedFor(), rf.me, req.CandidateId)
		}
	} else { //当前的日志term要比请求的小
		reply.VoteGranted = true
		rf.setVotedFor(req.CandidateId)
		rf.becomeFollower(NONE)
		rf.setElectionTimeOut()
		DPrintf("term2=%d,role=%s,rf=%d同意rf=%d的选举请求,req的日志term比较大", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, req.CandidateId)
	}
}

func (rf *Raft) setSwitchLeaderHeartbeatChecker(val bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderHeartCheckerSwitch = val
}

func (rf *Raft) getSwitchLeaderHeartbeatCheckerFlag() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderHeartCheckerSwitch
}

/*
该方法的发起者只可能是leader，或者说，自认为的leader
Q:如果是leader接收到了心跳如何处理
A:如果请求的term比自己大，那么就退回follower状态

Q:候选者接收到了心跳如何处理
A:接收到了leader的心跳，那么直接退回follower
*/
func (rf *Raft) AppendEntries(req *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = req.Term
	if len(req.Entries) == 0 { //是心跳操作
		if rf.getRaftRole() == CANDIDATE {
			rf.setRaftRole(FOLLOWER)
			rf.setElectionTimeOut()
			rf.setLastLeaderHeartBeatTime()
			rf.setVotedFor(NONE)
		}
		if rf.getRaftTerm() <= req.Term {
			DPrintf("term=%d,role=%s,rf=%d确认来自rf=%d的心跳请求,reqTerm=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, req.Me, req.Term)
			//leader发现了更大的term
			if rf.getRaftTerm() < req.Term && rf.getRaftRole() == LEADER {
				rf.becomeFollower(NONE)
				DPrintf("term=%d,role=%s,rf=%d 由leader退化为follower", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me)
			}
			rf.setLastLeaderHeartBeatTime()
			rf.setRaftTerm(req.Term)
			reply.Succ = true
			rf.setVotedFor(req.Me)
			if !rf.getSwitchLeaderHeartbeatCheckerFlag() {
				rf.setSwitchLeaderHeartbeatChecker(true)
				go rf.leaderHeartbeatChecker()
			}
		} else {
			reply.Succ = false
			DPrintf("term=%d,role=%s,rf=%d拒绝来自rf=%d的心跳请求,reqTerm=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, req.Me, req.Term)
		}
	} else { //日志追加操作

	}
}

func getRole(i int) string {
	if i == 0 {
		return "FOLLOWER"
	} else if i == 1 {
		return "CANDICATE"
	} else if i == 2 {
		return "LEADER"
	} else {
		return "NULl"
	}

}

func (rf *Raft) leaderHeartbeatChecker() {
	for {
		time.Sleep(time.Duration(rf.raftHeartBeatIntervalMilli) * time.Millisecond)
		timeElapse := GetNowMilliTime() - rf.getLastLeaderHeartBeatTime()
		if int(timeElapse) > rf.raftHeartBeatIntervalMilli {
			if !(timeElapse > 200) {
				DPrintf("term=%d,role=%s,rf=%d votedFor=%d的心跳超时未连接,重置归属,timeEla=%d,hbintevl=%d",
					rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, rf.getVotedFor(), timeElapse, rf.raftHeartBeatIntervalMilli)
			}
			rf.setVotedFor(NONE)
			//			rf.setElectionTimeOut()
			//			rf.setLastLeaderHeartBeatTime()
			//			rf.setSwitchLeaderHeartbeatChecker(false)
			//			return
		} else {
			//				DPrintf("term=%d,role=%s,rf=%d,timeEla=%d，未出现心跳超时的情况", rf.getRaftTerm(), rf.me, timeElapse)
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

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.

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

	// Your initialization code here (2A, 2B, 2C).
	//初始化当前参数
	rf.initParam()
	//开始选举超时判定任务
	go rf.electionTimeOutTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

//获取当前时间的毫秒数
func GetNowMilliTime() int64 {
	return time.Now().UnixNano() / 1000000
}

func (rf *Raft) electionTimeOutTimer() {
	for {
		time.Sleep(time.Duration(rf.getElectionTimeOut()) * time.Millisecond)
		if rf.getRaftRole() == LEADER {
			continue
		}
		nowTime := GetNowMilliTime()
		if int(nowTime-rf.getLastLeaderHeartBeatTime()) > rf.getElectionTimeOut() {
			rf.startElection()
		}
	}
}

/*
该方法只能是候选者发起，如果检测到了更大的term，那么就返回到follower状态
*/
func (rf *Raft) startElection() {
	if rf.getRaftRole() != CANDIDATE {
		rf.setRaftRole(CANDIDATE)
	}
	succ := 0
	majority := (len(rf.peers) + 1) / 2
	rf.setRaftTerm(rf.getRaftTerm() + 1)
	rf.setElectionTimeOut()
	rf.setVotedFor(NONE)
	n := len(rf.peers)
	count := 0
	voteResult := make(chan *RequestVoteReply, n)
	DPrintf("term=%d,role=%s,rf=%d发起了一次选举,timeOut=%d,votedFor=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, rf.getElectionTimeOut(), rf.getVotedFor())
	for index := range rf.peers {
		tmpIndex := index
		go func() {
			voteArgs := &RequestVoteArgs{}
			voteArgs.Term = rf.getRaftTerm()
			voteArgs.CandidateId = rf.me
			voteArgs.LastLogIndex = 0
			voteArgs.LastLogTerm = 0
			voteReply := &RequestVoteReply{}
			result := rf.sendRequestVote(tmpIndex, voteArgs, voteReply)
			if result {
				voteResult <- voteReply
			} else {
				voteResult <- nil
			}
		}()

	}
	//如果所有节点都连不上网，那么就恢复当前的term?
	networkError := 0
	biggerTermFlag := false
	for reply := range voteResult {
		count++
		if reply == nil {
			networkError++
		} else if reply.VoteGranted {
			succ++
		}
		if reply != nil && reply.Term > rf.getRaftTerm() {
			biggerTermFlag = true
			rf.setRaftTerm(reply.Term)
		}
		if count == n || succ >= majority || networkError >= majority {
			break
		}
	}
	DPrintf("term=%d,role=%s,rf=%d 选举结果收集succ=%d,networkError=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, succ, networkError)
	//Q:如何判定在收集候选者期间，已经有leader联系自己了
	//A:存在leader的情况下，选举必然失败，那么可以通过leader的心跳感知，然后退回follower
	if succ >= majority {
		rf.setRaftRole(LEADER)
		rf.setVotedFor(rf.me)
		DPrintf("term=%d,role=%s,rf=%d 成为leader", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me)
		go rf.maintainLeader()
	} else {
		if biggerTermFlag {
			DPrintf("term=%d,role=%s,rf=%d 候选者检测到了更高的term，退化为follower")
			rf.setRaftRole(FOLLOWER)
			rf.setVotedFor(NONE)
			rf.setLastLeaderHeartBeatTime()
			rf.setElectionTimeOut()
		} else {
			//			rf.setVotedFor(NONE)
			DPrintf("term=%d,role=%s,rf=%d 竞选leader失败,succ=%d,networkError=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, succ, networkError)
		}
	}

}

//Q leader在什么情况下，会退化为follower。也就是维持leader地位的协程何时停止
//A 发送心跳发现有比自己term更高的时候；有人向leader发起投票请求，然后leader感知到了更高的term；有leader给自己发送心跳请求的时候
func (rf *Raft) maintainLeader() {
	for {
		if rf.getRaftRole() != LEADER {
			DPrintf("term=%d,rf=%d 监测到当前角色为已经不是leader，退出leader心跳循环", rf.getRaftTerm(), rf.me)
			return
		}
		req := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}
		begin := GetNowMilliTime()
		req.LeaderCommit = rf.commitIndex
		req.Term = rf.getRaftTerm()
		req.LeaderId = rf.me
		req.Me = rf.me

		currentTerm := req.Term
		n := len(rf.peers)
		//		majority := (len(rf.peers) + 1) / 2
		chnResult := make(chan *AppendEntriesReply, n)
		DPrintf("term=%d,role=%s,rf=%d 开始发送leader心跳", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me)
		for index := range rf.peers {
			tmpIndex := index
			//			if tmpIndex == rf.me {
			//				ry := &AppendEntriesReply{}
			//				ry.Succ = true
			//				chnResult <- ry //心跳成功
			//				continue
			//			}
			//			tmp := index
			go func() {
				timeoutChn := make(chan int)
				appendEntryResultChn := make(chan bool)
				go func() {
					time.Sleep(time.Duration(rf.raftHeartBeatIntervalMilli*2) * time.Millisecond)
					timeoutChn <- 1
				}()
				go func() {
					result := rf.sendAppendEntries(tmpIndex, req, reply)
					appendEntryResultChn <- result
				}()
				select {
				case result := <-appendEntryResultChn:
					if result {
						chnResult <- reply
					} else {
						chnResult <- nil //网络失败
					}
				case <-timeoutChn:
					chnResult <- nil
				}
				//				if result {
				//					chnResult <- reply
				//				} else {
				//					chnResult <- nil //网络失败
				//				}
			}()

		}
		tmp := 0

		//当大多数的心跳和网络请求失败，那么需要考虑当前节点退回follower的情况
		succ := 0
		networkError := 0
		//当发生网络异常的时候，心跳请求可能无法返回，所以需要对请求附加超时机制?
		biggerTermFlag := false
		for reply := range chnResult {
			tmp++
			if reply == nil {
				networkError++
			} else if reply.Succ {
				succ++
			}
			if reply != nil && reply.Term > rf.getRaftTerm() {
				rf.setRaftTerm(reply.Term)
				biggerTermFlag = true
			}
			if tmp == n {
				break
			}
		}
		if biggerTermFlag {
			DPrintf("term=%d,role=%d,rf=%d 检测到了更大的term，由leader退回follower，发起心跳时的term=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, currentTerm)
			rf.becomeFollower(NONE)
			rf.setElectionTimeOut()
			rf.setLastLeaderHeartBeatTime()
			return
		}
		if networkError == n {
			rf.becomeFollower(NONE)
			rf.setElectionTimeOut()
			rf.setLastLeaderHeartBeatTime()
			DPrintf("term=%d,role=%d,rf=%d 当前节点网络超时，由leader退回follower,耗时=%d，失联时的term为=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, GetNowMilliTime()-begin, currentTerm)
			return
		}
		DPrintf("term=%d,role=%s,rf=%d leader心跳已完成,succ=%d,networkError=%d，发起心跳时的term=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, succ, networkError, currentTerm)
		//		if succ < majority {
		//			DPrintf("term=%d rf=%d 退化成为Follower", rf.getRaftTerm(), rf.me)
		//			rf.becomeFollower(NONE)
		//			return
		//		}
		//		if rf.getRaftRole() == FOLLOWER {
		//			DPrintf("term=%d,rf=%d 监测到当前角色为FOLLOWER，退出leader心跳循环", rf.getRaftTerm(), rf.me)
		//			return
		//		}
		time.Sleep(time.Duration(rf.raftHeartBeatIntervalMilli) * time.Millisecond)
	}
}

//candidate -> follower
//leader -> follower
func (rf *Raft) becomeFollower(candidateId int) {
	rf.setVotedFor(candidateId)
	rf.setRaftRole(FOLLOWER)
	//	rf.setElectionTimeOut()
}

func (rf *Raft) initParam() {
	rf.setRaftTerm(0)
	rf.commitIndex = 0
	rf.setRaftRole(FOLLOWER)
	rf.setElectionTimeOut()
	rf.setVotedFor(NONE)
	rf.raftHeartBeatIntervalMilli = 100
	rf.setLastLeaderHeartBeatTime()
	rf.entries = make([]LogEntry, 10)
	rf.leaderHeartCheckerSwitch = false
}

func (rf *Raft) setLastLeaderHeartBeatTime() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastLeaderHeartBeatTime = GetNowMilliTime()
}

func (rf *Raft) getLastLeaderHeartBeatTime() int64 {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastLeaderHeartBeatTime
}

func (rf *Raft) setElectionTimeOut() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeoutMS = produceElectionTimeoutParam()
}

func (rf *Raft) getElectionTimeOut() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electionTimeoutMS
}

func produceElectionTimeoutParam() int {
	return RandInt(250, 350)
}

func RandInt(start, end int) int {
	rand.Seed(time.Now().UnixNano())
	cha := end - start
	n := rand.Intn(cha)
	return n + start
}

func (rf *Raft) setVotedFor(votedFor int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = votedFor
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) getRaftTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setRaftTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}
