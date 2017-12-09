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
	leaderId                   int  //当前raft节点认为的leader的id
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(req *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.getRaftTerm()
	if req.Term < rf.getRaftTerm() {
		reply.VoteGranted = false
		return
	}

	lastEntry := rf.entries[len(rf.entries)-1]
	if lastEntry.Term > req.LastLogTerm {
		reply.VoteGranted = false
		return
	} else if lastEntry.Term == req.LastLogTerm {
		if rf.commitIndex <= req.LastLogIndex {
			rf.setRaftRole(FOLLOWER)
			//			println(rf.me, "投票赞成", req.CandidateId)
			rf.setRaftLeaderId(req.CandidateId)
			reply.VoteGranted = true
			rf.setRaftTerm(req.Term)
		} else {
			reply.VoteGranted = false
		}
	} else {
		rf.setRaftRole(FOLLOWER)
		rf.setRaftLeaderId(req.CandidateId)
		reply.VoteGranted = true
		rf.setRaftTerm(req.Term)
	}

}

func (rf *Raft) AppendEntries(req *AppendEntriesArgs, reply *AppendEntriesReply) {
	if len(req.Entries) == 0 {
		if rf.getRaftTerm() <= req.Term {
			rf.setLastLeaderHeartBeatTime()
			reply.Succ = true
		} else {
			reply.Succ = false
		}
	} else {

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
	//	println("debug1909", args == nil, reply == nil)
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
		//debug
		//		println(rf.me, "选举超时时间：", rf.electionTimeoutMS)
		time.Sleep(time.Duration(rf.electionTimeoutMS) * time.Millisecond)
		if rf.getRaftRole() == LEADER {
			continue
		}
		//debug
		//		println(rf.me, "准备开始选举leader,leaderId=", rf.getRaftLeaderId(), "role=", rf.getRaftRole())
		nowTime := GetNowMilliTime()
		if int(nowTime-rf.getLastLeaderHeartBeatTime()) > rf.electionTimeoutMS || rf.getRaftRole() == CANDIDATE {
			go rf.startElection()
		}
	}
}

func (rf *Raft) startElection() {
	if rf.getRaftRole() != CANDIDATE {
		rf.setRaftRole(CANDIDATE)
	}
	//	println(rf.me, ",role=", rf.getRaftRole(), ",leaderId=", rf.getRaftLeaderId())
	//	rf.setRaftLeaderId(NONE)
	succ := 0
	majority := (len(rf.peers) + 1) / 2
	rf.setRaftTerm(rf.getRaftTerm())
	rf.setElectionTimeOut()
	n := len(rf.peers)
	count := 0
	voteResult := make(chan int, n)
	for index := range rf.peers {
		tmpIndex := index
		if tmpIndex == rf.me {
			voteResult <- 1
			continue
		}
		go func() {
			voteArgs := &RequestVoteArgs{}
			voteArgs.Term = rf.getRaftTerm()
			voteArgs.CandidateId = rf.me
			voteArgs.LastLogIndex = 0
			voteArgs.LastLogTerm = 0

			voteReply := &RequestVoteReply{}
			//			println("index1=", tmpIndex, "rf.me123=", rf.me)
			result := rf.sendRequestVote(tmpIndex, voteArgs, voteReply)
			if result {
				if voteReply.VoteGranted {
					voteResult <- 1
				} else {
					if voteReply.Term > rf.getRaftTerm() {
						rf.setRaftTerm(voteReply.Term)
						voteResult <- 0
					}
				}
			} else {
				println("尝试连接server %d时，网络连接异常", tmpIndex)
				voteResult <- 2
			}
		}()

	}
	//	networkError := 0
	for a := range voteResult {
		count++
		if a == 1 {
			succ++
		}
		if count == n {
			break
		}
	}
	//	println("网络连接失败的个数：", networkError)
	//Q:如何判定在收集候选者期间，已经有leader联系自己了
	//A:为当前raft设计一个leaderId，代表当前raft承认的leader
	if rf.getRaftLeaderId() > 0 {
		rf.becomeFollower(rf.getRaftLeaderId())
	} else if succ >= majority {
		rf.setRaftRole(LEADER)
		println(rf.me, "赢得选举")
		go rf.maintainLeader()
	} else {
		rf.setRaftRole(CANDIDATE)
	}
}

//Q leader在什么情况下，会退化为follower。也就是维持leader地位的协程何时停止
//A 1，心跳请求各种失败的时候，如果可以联通的话，就是term值不能大于大多数，那么退回follower；
// 如果大多数都无法联通，说明可能是网络出了问题，那么就退回follower
func (rf *Raft) maintainLeader() {
	for {

		time.Sleep(time.Duration(rf.raftHeartBeatIntervalMilli) * time.Millisecond)
		req := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}

		req.LeaderCommit = rf.commitIndex
		req.Term = rf.getRaftTerm()
		req.LeaderId = rf.me

		n := len(rf.peers)
		majority := (len(rf.peers) + 1) / 2
		chnResult := make(chan int, n)

		for index := range rf.peers {
			tmpIndex := index
			if tmpIndex == rf.me {
				chnResult <- 1 //心跳成功
				continue
			}
			//			tmp := index
			go func() {
				//				println(rf.me, "2016,index=", tmp)
				result := rf.sendAppendEntries(tmpIndex, req, reply)
				//				println(rf.me, "2017,index=", tmp, "result=", result)
				if result {
					if !reply.Succ {
						chnResult <- 0 //心跳失败
					} else {
						chnResult <- 1 //心跳成功
					}

				} else {
					chnResult <- 2 //网络失败
					println("向发送心跳失败,source=", rf.me, "target=", tmpIndex)
					//					println(rf.me, "=3004")
				}
			}()

		}
		tmp := 0

		//当大多数的心跳和网络请求失败，那么需要考虑当前节点退回follower的情况
		maintainHeartBeatFail := 0
		networkError := 0
		//当发生网络异常的时候，心跳请求可能无法返回，所以需要对请求附加超时机制
		for r := range chnResult {
			tmp++
			//			println(rf.me, "等待心跳结果的返回，", tmp)
			if r == 0 {
				maintainHeartBeatFail++
			}
			if r == 2 {
				networkError++
			}
			if tmp == n {
				break
			}
		}

		if maintainHeartBeatFail >= majority || networkError >= majority {
			println(rf.me, "退化为follower")
			rf.becomeFollower(NONE)
			break
		}
		//		println(rf.me, "完成一次心跳循环")
	}
}

//candidate -> follower
//leader -> follower
func (rf *Raft) becomeFollower(leaderId int) {
	rf.setRaftLeaderId(leaderId)
	rf.setRaftRole(FOLLOWER)
	rf.setElectionTimeOut()
}

func (rf *Raft) initParam() {
	rf.setRaftTerm(0)
	rf.commitIndex = 0
	rf.setRaftRole(FOLLOWER)
	rf.setElectionTimeOut()
	rf.setRaftLeaderId(NONE)
	rf.raftHeartBeatIntervalMilli = 50
	rf.setLastLeaderHeartBeatTime()
	rf.entries = make([]LogEntry, 10)
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

func produceElectionTimeoutParam() int {
	return RandInt(150, 300)
}

func RandInt(start, end int) int {
	cha := end - start
	n := rand.Intn(cha)
	return n + start
}

func (rf *Raft) setRaftLeaderId(leaderId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leaderId = leaderId
}

func (rf *Raft) getRaftLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderId
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
