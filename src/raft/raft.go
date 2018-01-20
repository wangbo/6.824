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
	Command interface{}
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
	//	IsLogMatch bool //标记当前日志追加失败的操作是否为日志不匹配引起的
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
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	electionTimeoutMS          int   //选举超时时间，时间单位为毫秒
	nodeRole                   int   //当前节点的角色状态，默认为follower
	currentTerm                int   //当前的term
	commitIndex                int   //当前已提交的index
	currentIndex               int   //已经提交到状态机的index
	lastLeaderHeartBeatTime    int64 //上次心跳时，unix时间戳，单位是毫秒
	raftHeartBeatIntervalMilli int   //raft心跳间隔，单位是毫秒，任务初始化时指定
	raftIsShutdown             bool  //当前进程是否关闭
	votedFor                   int   //当前term投票给谁了
	leaderHeartCheckerSwitch   bool  //用于检测在leader心跳的开关

	//	raftIsShutdown     bool       //是否退出当前raft
	raftIsShutdownLock sync.Mutex //是否关闭的lock
	logEntries         []LogEntry //保存接收到的日志
	logEntryLock       sync.Mutex //同步日志时使用的锁

	nextIndexMap      map[int]int //下一个需要和leader同步的日志的index
	nextIndexMapLock  sync.Mutex
	nextIndexFlagMap  map[int]int //标志位，如果为1,则表示当前有协程在处理和follower的同步操作，如果为0则表示没有
	nextIndexFlagLock sync.Mutex  //更新nextIndexFlagMap时需要保证线程安全

	appendEntrySerialLock sync.Mutex //保证leader追加日志的操作串行，上一波日志没有提交完成，下一波不能提交
	maitainLeaderLock     sync.Mutex //为maintainleader的操作串行化

	opCount     int
	opCountLock sync.Mutex
}

func (rf *Raft) getLogEntryLength() int {
	rf.logEntryLock.Lock()
	defer rf.logEntryLock.Unlock()
	return len(rf.logEntries)
}

func (rf *Raft) initNextIndex() {
	rf.nextIndexFlagLock.Lock()
	defer rf.nextIndexFlagLock.Unlock()
	rf.nextIndexMap = make(map[int]int)
	rf.nextIndexFlagMap = make(map[int]int)
	for i := range rf.peers {
		//		rf.nextIndexMap[i] = rf.getLogEntryLength()
		rf.setNextIndexMapValue(i, rf.getLogEntryLength())
		rf.nextIndexFlagMap[i] = 0
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	isleader = false
	// Your code here (2A).
	if rf.getRaftRole() == LEADER {
		isleader = true
	}
	term = rf.getRaftTerm()
	return term, isleader
}

func (rf *Raft) setRaftIsShutdown(lock bool) {
	rf.raftIsShutdownLock.Lock()
	defer rf.raftIsShutdownLock.Unlock()
	rf.raftIsShutdown = lock
}

func (rf *Raft) getRaftIsShutdown() bool {
	rf.raftIsShutdownLock.Lock()
	defer rf.raftIsShutdownLock.Unlock()
	return rf.raftIsShutdown
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
	//	tmpVotedFor := rf.getVotedFor()
	//	rf.setLastLeaderHeartBeatTime()
	if rf.getRaftTerm() > req.Term {
		DPrintf("term=%d,role=%s,rf=%d 拒绝rf=%d的投票请求，因为req的term比较小,reqTerm=%d", rf.getRaftTerm(),
			getRole(rf.getRaftRole()), rf.me, req.CandidateId, req.Term)
		return
	}
	//当前的raft是有主的，那么必然要拒绝其他选举请求
	if rf.getVotedFor() >= 0 {
		DPrintf("term=%d,role=%s,rf=%d 投过了票所以拒绝了rf=%d的选举请求,votedFor=%d,reqTerm=%d", rf.getRaftTerm(),
			getRole(rf.getRaftRole()), rf.me, req.CandidateId, rf.getVotedFor(), req.Term)
		reply.VoteGranted = false
		return
	}
	if rf.getRaftTerm() < req.Term {
		rf.setRaftTerm(req.Term)
		rf.setRaftRole(FOLLOWER)
	}

	lastEntry := rf.getLastLogEntry()
	if lastEntry.Term > req.LastLogTerm {
		reply.VoteGranted = false
		return
	} else if lastEntry.Term == req.LastLogTerm {
		if lastEntry.Index <= req.LastLogIndex {
			reply.VoteGranted = true
			rf.becomeFollower(req.CandidateId)
			//			DPrintf("term=%d,role=%s,votedFor=%d,rf=%d同意了rf=%d的选举请求,之前的voteFor为%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.getVotedFor(), rf.me, req.CandidateId, tmpVotedFor)
		} else {
			reply.VoteGranted = false
			//			DPrintf("term=%d,role=%s,votedFor=%d,rf=%d拒绝了rf=%d的选举请求,req的日志长度比较小", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.getVotedFor(), rf.me, req.CandidateId)
		}
	} else { //当前的日志term要比请求的小
		reply.VoteGranted = true
		rf.becomeFollower(req.CandidateId)
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
	reply.Term = rf.getRaftTerm()
	if len(req.Entries) == 0 { //是心跳操作
		//		if rf.getRaftRole() == CANDIDATE {
		//			rf.becomeFollower(NONE)
		//		}
		if rf.getRaftTerm() > req.Term {
			reply.Succ = false
			DPrintf("term=%d,role=%s,rf=%d拒绝来自rf=%d的心跳请求,reqTerm=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, req.Me, req.Term)
		} else {
			//			DPrintf("term=%d,role=%s,rf=%d确认来自rf=%d的心跳请求,reqTerm=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, req.Me, req.Term)
			if rf.getRaftTerm() == req.Term && rf.getRaftRole() == LEADER {
				if rf.me == req.LeaderId {
					rf.setLastLeaderHeartBeatTime()
					reply.Succ = true
				} else {
					AllPrintf("term=%d,role=%s,rf=%d 拒绝来自%d的心跳，因为term相等", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, req.LeaderId)
					reply.Succ = false
				}
			} else {
				currentRole := rf.getRaftRole()
				rf.setRaftTerm(req.Term)
				reply.Succ = true
				rf.becomeFollower(req.Me)
				if rf.getRaftRole() != FOLLOWER {
					DPrintf("term=%d,role=%s,rf=%d 由%s退化为follower", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, getRole(currentRole))
				}

				//同步与leader的commitedIndex
				if req.LeaderCommit > rf.getCommitIndex() {
					recvLastEntryIndex := rf.getLogEntryLength() - 1
					if recvLastEntryIndex < req.LeaderCommit { //取较小应该是为了不越界
						rf.updateCommitIndex(recvLastEntryIndex)
					} else {
						rf.updateCommitIndex(req.LeaderCommit)
					}
					DPrintf("term=%d,role=%s,rf=%d 更新commitIndex=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, rf.getCommitIndex())
				}
				DPrintf("term=%d,role=%s,rf=%d 确认%d的心跳,req.Term=%d,rf.commitIndex=%d,req.commitIndex=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, req.LeaderId, req.Term, rf.getCommitIndex(), req.LeaderCommit)
			}
			rf.mu.Lock()
			if !rf.leaderHeartCheckerSwitch {
				rf.leaderHeartCheckerSwitch = true
				go rf.leaderHeartbeatChecker()
			}
			rf.mu.Unlock()
		}
	} else { //日志追加操作
		rf.logEntryLock.Lock()
		defer rf.logEntryLock.Unlock()
		reply.Term = rf.getRaftTerm()
		//在这里可以拒绝掉过时leader的请求，过时的leader会因此退回follower
		if req.Term < rf.getRaftTerm() {
			reply.Succ = false
			return
		}

		//日志匹配,leader的prevLogIndex是否会大于follower的长度，防止出现数组越界异常
		if len(rf.logEntries)-1 < req.PrevLogIndex {
			BPrintf("term=%d,role=%s,rf=%d req.PrevLogIndex=%d > len(rf.logEntries)=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, req.PrevLogIndex, len(rf.logEntries)-1)
			reply.Succ = false
			return
		}

		probablyLogEntry := rf.logEntries[req.PrevLogIndex]
		if probablyLogEntry.Term == req.PrevLogTerm && probablyLogEntry.Index == req.PrevLogIndex {
			newLogEntry := req.Entries[0]
			rfPrevLogIndex := len(rf.logEntries) - 1
			if req.PrevLogIndex != rfPrevLogIndex {
				rf.logEntries = rf.logEntries[:req.PrevLogIndex+1]
				BPrintf("term=%d,role=%s,rf=%d 删除冲突日志,cmd=%d,req.PreLogIndex=%d,len(rf.log)=%d,leader=%d",
					rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, newLogEntry.Command.(int),
					req.PrevLogIndex, rfPrevLogIndex, rf.votedFor)
			}
			rf.logEntries = append(rf.logEntries, newLogEntry)
			reply.Succ = true
			rf.setLastLeaderHeartBeatTime()
			//			if recvLastEntryIndex < req.LeaderCommit { //取较小应该是为了不越界
			//				rf.updateCommitIndex(recvLastEntryIndex)
			//			} else {
			//				rf.updateCommitIndex(req.LeaderCommit)
			//			}
			BPrintf("term=%d,role=%s,rf=%d 认同leader的日志追加操作,entry.cmd=%d,entry.index=%d,len(rf.logEntries)=%d,commitedIndex=%d",
				rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, newLogEntry.Command.(int), newLogEntry.Index, len(rf.logEntries), rf.getCommitIndex())
		} else {
			reply.Succ = false
			BPrintf("term=%d,role=%s,rf=%d 指定位置的term不匹配，拒绝追加操作,cmd=", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me,
				req.Entries[0].Command.(int))
		}
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
		return "NULL"
	}

}

func (rf *Raft) leaderHeartbeatChecker() {
	for !rf.getRaftIsShutdown() {
		time.Sleep(time.Duration(rf.raftHeartBeatIntervalMilli) * time.Millisecond)
		timeElapse := GetNowMilliTime() - rf.getLastLeaderHeartBeatTime()
		if int(timeElapse) > rf.raftHeartBeatIntervalMilli {
			if !(timeElapse > 200) {
				DPrintf("term=%d,role=%s,rf=%d votedFor=%d的心跳超时未连接,重置归属,timeEla=%d,hbintevl=%d",
					rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, rf.getVotedFor(), timeElapse, rf.raftHeartBeatIntervalMilli)
			}
			rf.setVotedFor(NONE)
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

func (rf *Raft) sendAppendEntriesWithTimeout(server int, req *AppendEntriesArgs, reply *AppendEntriesReply, timeoutMili int) bool {
	result := make(chan bool)
	go func() {
		time.Sleep(time.Duration(timeoutMili) * time.Millisecond)
		result <- false
	}()
	go func() {
		result <- rf.sendAppendEntries(server, req, reply)
	}()
	return <-result
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
	isLeader := false

	// Your code here (2B).
	//	rf.mu.Lock()
	//	defer rf.mu.Unlock()
	rf.appendEntrySerialLock.Lock()
	defer rf.appendEntrySerialLock.Unlock()
	index = rf.getLogEntryLength()
	term = rf.getRaftTerm()
	if rf.nodeRole == LEADER {
		//		BPrintf("找打了leader,%d,role=%s", rf.me, getRole(rf.nodeRole))
		isLeader = true
		newLogEntry := rf.appendLogEntry(command)
		rf.leaderSendAppendEntries(newLogEntry)
		//		go rf.leaderSendAppendEntries(newLogEntry)
		//		rf.leaderSendAppendEntries(command)
		//		println("applSucc,cmd=", command.(int))
	}
	//	println("applSucc,cmd=", command.(int))
	return index, term, isLeader
}

func (rf *Raft) getLogEntryByCommand(command interface{}) LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var logEntry LogEntry
	logEntry.Command = command
	logEntry.Index = len(rf.logEntries)
	logEntry.Term = rf.currentTerm
	return logEntry
}

func (rf *Raft) updateCommitIndex(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = index
}

func (rf *Raft) getCommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func chanIsClosed(ch <-chan *AppendEntriesReply) bool {
	select {
	case <-ch:
		return true
	default:
	}
	return false
}

func (rf *Raft) lockFollowerAppendEntry(tmpI int) bool {
	rf.nextIndexFlagLock.Lock()
	defer rf.nextIndexFlagLock.Unlock()
	if rf.nextIndexFlagMap[tmpI] == 0 {
		rf.nextIndexFlagMap[tmpI] = 1
		return true
	} else {
		return false
	}
}

func (rf *Raft) unlockFollowerAppendEntry(tmpI int) {
	rf.nextIndexFlagLock.Lock()
	defer rf.nextIndexFlagLock.Unlock()
	rf.nextIndexFlagMap[tmpI] = 0
}

func (rf *Raft) appendLogEntry(command interface{}) LogEntry {
	rf.logEntryLock.Lock()
	defer rf.logEntryLock.Unlock()
	var logEntry LogEntry
	logEntry.Command = command
	logEntry.Index = len(rf.logEntries)
	logEntry.Term = rf.getRaftTerm()
	//	logEntry.Term = rf.currentTerm
	rf.logEntries = append(rf.logEntries, logEntry)
	return logEntry
}

func (rf *Raft) incrementOpCount() int {
	rf.opCountLock.Lock()
	defer rf.opCountLock.Unlock()
	rf.opCount++
	return rf.opCount
}

func (rf *Raft) setNextIndexMapValue(rfIndex int, value int) {
	rf.nextIndexMapLock.Lock()
	defer rf.nextIndexMapLock.Unlock()
	rf.nextIndexMap[rfIndex] = value
}

func (rf *Raft) getNextIndexMapValue(rfIndex int) int {
	rf.nextIndexMapLock.Lock()
	defer rf.nextIndexMapLock.Unlock()
	return rf.nextIndexMap[rfIndex]
}

func (rf *Raft) setNextIndexMapValueIncrement(rfIndex int) {
	rf.nextIndexMapLock.Lock()
	defer rf.nextIndexMapLock.Unlock()
	rf.nextIndexMap[rfIndex]++
}

func (rf *Raft) setNextIndexMapValueDecrement(rfIndex int) {
	rf.nextIndexMapLock.Lock()
	defer rf.nextIndexMapLock.Unlock()
	rf.nextIndexMap[rfIndex]--
}

//在leader的心跳中，更新已提交的commitIndex
//生产环境应该需要添加重试机制
func (rf *Raft) leaderSendAppendEntries(newLogEntry LogEntry) {
	//	rf.appendEntrySerialLock.Lock()
	//	defer rf.appendEntrySerialLock.Unlock()
	//每次优先追加日志，然后再说同步的事 why
	//	newLogEntry := rf.appendLogEntry(command)
	BPrintf("term=%d role=%s rf=%d 开始追加日志，cmd=%d,index=%d,term=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, newLogEntry.Command.(int), newLogEntry.Index, newLogEntry.Term)

	begin := GetNowMilliTime()

	//	tmpNextIndex := rf.getLogEntryLength() - 1
	tmpNextIndex := newLogEntry.Index
	currentSubmitEndIndex := tmpNextIndex + 1

	//	BPrintf("term=%d role=%s rf=%d 开始追加日志，cmd=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, command)
	appendEntryResult := make(chan *AppendEntriesReply, len(rf.peers)-1)
	for i := range rf.peers {
		tmpI := i
		if tmpI != rf.me {
			if !rf.lockFollowerAppendEntry(tmpI) {
				continue
			}
			go func() {
				//初始化下一个发送到follower的entry
				//				rf.nextIndexMap[tmpI] = tmpNextIndex
				initTerm := rf.getRaftTerm()
				rf.setNextIndexMapValue(tmpI, tmpNextIndex)
				for !rf.getRaftIsShutdown() && rf.getRaftRole() == LEADER {
					begin0 := GetNowMilliTime()
					req := &AppendEntriesArgs{}
					req.Term = rf.getRaftTerm()
					req.LeaderId = rf.me
					req.LeaderCommit = rf.getCommitIndex()

					rfNextIndexMapValue := rf.getNextIndexMapValue(tmpI)
					req.PrevLogIndex = rfNextIndexMapValue - 1

					if req.PrevLogIndex >= rf.getLogEntryLength() || req.PrevLogIndex < 0 {
						BPrintf("term=%d role=%s rf =%d 连接%d，req.PrevLogIndex捕捉到数组越界异常,req.PrevIndex=%d,len(rf.logEntry)=%d,nextIndex=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, tmpI, req.PrevLogIndex, rf.getLogEntryLength(), rfNextIndexMapValue)
					}
					req.PrevLogTerm = rf.getTargetIndexLogEntry(req.PrevLogIndex).Term
					//这里会有越界异常
					if rfNextIndexMapValue >= rf.getLogEntryLength() || rfNextIndexMapValue < 0 {
						BPrintf("term=%d role=%s rf =%d 连接%d，捕捉到数组越界异常,nextIndex=%d,len(rf.logEntry)=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, tmpI, rfNextIndexMapValue, rf.getLogEntryLength())
					}
					nextLogEntry := rf.getTargetIndexLogEntry(rfNextIndexMapValue)

					BPrintf("term=%d role=%s rf =%d 连接%d,entry.Index=%d,entry.Cmd=%d,len(rf.logEntry)=%d,nextIndex=%d",
						rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, tmpI, nextLogEntry.Index, nextLogEntry.Command.(int), rf.getLogEntryLength(), rfNextIndexMapValue)
					req.Entries = append(req.Entries, nextLogEntry)
					reply := &AppendEntriesReply{}
					//					result := rf.sendAppendEntries(tmpI, req, reply)
					//					rf.sendAppendEntriesWithTimeout()
					result := rf.sendAppendEntriesWithTimeout(tmpI, req, reply, rf.electionTimeoutMS)
					if !result { //网络异常，重试
						if initTerm != rf.getRaftTerm() {
							BPrintf("term=%d role=%s rf =%d 连接%d时检测到term变化，退出当前appendEntry循环，初始term=%d,当前term=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, tmpI, initTerm, rf.getRaftTerm())
							reply.Succ = false
							reply.Term = -999
							appendEntryResult <- reply
							break
						}
						BPrintf("term=%d role=%s rf =%d 连接%d时网络异常，发起重试请求，耗时=%d，发起请求时的term为%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, tmpI, GetNowMilliTime()-begin0, initTerm)
						continue
					}
					//如果是term比较小的原因，那么leader就需要退回follower了
					if reply.Term > rf.getRaftTerm() {

						//						BPrintf("term=%d role=%s rf =%d 请求%d追加日志的操作成功", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, tmpI)
						//						if !chanIsClosed(appendEntryResult) {
						appendEntryResult <- reply
						//						}
						break
					} else if reply.Succ {
						//						rf.nextIndexMap[tmpI]++
						rf.setNextIndexMapValueIncrement(tmpI)
					} else if !reply.Succ { //如果不是因为term导致失败，那么一定是因为log不匹配
						//						rf.nextIndexMap[tmpI]--
						rf.setNextIndexMapValueDecrement(tmpI)
						BPrintf("term=%d role=%s rf =%d prevLogIndex与%d的不匹配，回退一格", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, tmpI)
					}
					tmpXX := rf.getNextIndexMapValue(tmpI)
					if tmpXX == currentSubmitEndIndex {
						//						if !chanIsClosed(appendEntryResult) {
						appendEntryResult <- reply
						//						}
						break
					}
				}
				rf.unlockFollowerAppendEntry(tmpI)
			}()
		}
	}
	succ := 1
	major := (len(rf.peers) + 1) / 2
	fail := 0
	for reply := range appendEntryResult {
		if reply.Succ == true {
			succ++
		} else if reply.Term > rf.getRaftTerm() {
			rf.setRaftTerm(reply.Term)
			rf.becomeFollower(NONE)
			BPrintf("term=%d,role=%s,rf=%d 在追加日志的过程中，发现更高的term，退回follower", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me)
			//			close(appendEntryResult)
			break
		} else if reply.Term == -999 {
			fail++
		}

		if succ >= major {
			//			BPrintf("term=%d,role=%s,rf=%d 大多数的节点已经接收到日志，耗时=%d,succ=", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, (GetNowMilliTime() - begin), succ)
			//			rf.applyMsg2StateMachine(newLogEntry)
			//这里加的可不一定是1啊兄弟
			rf.updateCommitIndex(rf.getLogEntryLength() - 1)
			//			close(appendEntryResult)
			break
		}
		if fail >= major {
			break
		}
	}
	BPrintf("term=%d,role=%s,rf=%d 完成日志追加操作，耗时=%d,succ=%d,commitIndex=%d,cmd=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, (GetNowMilliTime() - begin), succ, rf.getCommitIndex(), newLogEntry.Command.(int))
}

//模拟提交到状态机的操作
func (rf *Raft) applyMsg2StateMachine(entry LogEntry) {
	var applyMsg ApplyMsg
	applyMsg.Command = entry.Command
	applyMsg.Index = entry.Index
	rf.applyCh <- applyMsg
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.setRaftIsShutdown(true)
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	//初始化当前参数
	rf.initParam()
	//开始选举超时判定任务
	go rf.electionTimeOutTimer()

	//启动一个定时的任务，提交commited index之前的日志到状态机
	go rf.applyCommittedMsg2StateMachine()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) applyCommittedMsg2StateMachine() {
	for !rf.getRaftIsShutdown() {
		time.Sleep(time.Duration(5) * time.Millisecond)
		//		rf.mu.Lock()
		//		defer rf.mu.Unlock()
		commitIndex := rf.getCommitIndex()
		if rf.currentIndex == commitIndex {
			continue
		}
		//		BPrintf("term=%d,rf=%d,role=%s  applyMsg,logLen=%d,currentIndex=%d,commitIndex=%d", rf.getRaftTerm(), rf.me, getRole(rf.getRaftRole()),
		//			len(rf.logEntries), rf.currentIndex, rf.getCommitIndex())
		rf.logEntryLock.Lock()
		allCommitLen := len(rf.logEntries) - 1
		end := allCommitLen
		if commitIndex < allCommitLen {
			end = commitIndex
		}

		//		println(rf.currentIndex, end+1)
		toCommitArray := rf.logEntries[rf.currentIndex+1 : end+1]
		rf.logEntryLock.Unlock()
		for i := 0; i < len(toCommitArray); i++ {
			BPrintf("term=%d,applyMsgcccc_rf=%d,role=%s  applyMsgcccc,index=%d,cmd=%d,currentIndex=%d,commitIndex=%d,logLen=%d", rf.getRaftTerm(), rf.me, getRole(rf.getRaftRole()),
				toCommitArray[i].Index, toCommitArray[i].Command.(int), rf.currentIndex, rf.getCommitIndex(), allCommitLen)
			rf.applyMsg2StateMachine(toCommitArray[i])
			rf.currentIndex++
		}
		//		rf.currentIndex = rf.getCommitIndex()
		//		BPrintf("term=%d,applyMsgxxx_rf=%d,role=%s  applyMsgxxx,logLen=%d,currentIndex=%d,commitIndex=%d,logLen=%d", rf.getRaftTerm(), rf.me, getRole(rf.getRaftRole()),
		//			len(rf.logEntries), rf.currentIndex, rf.getCommitIndex(), allCommitLen)
	}
}

//获取当前时间的毫秒数
func GetNowMilliTime() int64 {
	return time.Now().UnixNano() / 1000000
}

func (rf *Raft) electionTimeOutTimer() {
	for !rf.getRaftIsShutdown() {
		time.Sleep(time.Duration(rf.getElectionTimeOut()) * time.Millisecond)
		if rf.getRaftRole() == LEADER {
			continue
		}
		nowTime := GetNowMilliTime()
		if int(nowTime-rf.getLastLeaderHeartBeatTime()) > rf.getElectionTimeOut() {
			rf.becomeCandicate()
			rf.startElection()
		}
	}
	DPrintf("term=%d,rf=%d,role=%d 执行关闭操作", rf.getRaftTerm(), rf.me, getRole(rf.getRaftRole()))
}

func (rf *Raft) getLastLogEntry() LogEntry {
	rf.logEntryLock.Lock()
	defer rf.logEntryLock.Unlock()
	return rf.logEntries[len(rf.logEntries)-1]
}

func (rf *Raft) getTargetIndexLogEntry(targetIndex int) LogEntry {
	rf.logEntryLock.Lock()
	defer rf.logEntryLock.Unlock()
	return rf.logEntries[targetIndex]
}

func chanIsClosed2(ch <-chan bool) bool {
	select {
	case <-ch:
		return true
	default:
	}
	return false
}

func (rf *Raft) sendRequestVoteWithTimeOut(server int, args *RequestVoteArgs, reply *RequestVoteReply, timeout int) bool {
	resultChan := make(chan bool)
	go func() {
		time.Sleep(time.Duration(timeout) * time.Millisecond)
		resultChan <- false
	}()
	go func() {
		resultChan <- rf.sendRequestVote(server, args, reply)
	}()
	select {
	case result := <-resultChan:
		//		close(resultChan)
		return result
	}
}

/*
该方法只能是候选者发起，如果检测到了更大的term，那么就返回到follower状态
*/
func (rf *Raft) startElection() {
	succ := 0
	majority := (len(rf.peers) + 1) / 2
	rf.setRaftTerm(rf.getRaftTerm() + 1)
	rf.setElectionTimeOut()
	n := len(rf.peers)
	count := 0
	voteResult := make(chan *RequestVoteReply, n)
	beginTime := GetNowMilliTime()
	DPrintf("term=%d,role=%s,rf=%d发起了一次选举,timeOut=%d,votedFor=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, rf.getElectionTimeOut(), rf.getVotedFor())
	for index := range rf.peers {
		tmpIndex := index
		go func() {
			voteArgs := &RequestVoteArgs{}
			voteArgs.Term = rf.getRaftTerm()
			voteArgs.CandidateId = rf.me
			lastLogEntry := rf.getLastLogEntry()
			voteArgs.LastLogIndex = lastLogEntry.Index
			voteArgs.LastLogTerm = lastLogEntry.Term
			voteReply := &RequestVoteReply{}
			//			result := rf.sendRequestVote(tmpIndex, voteArgs, voteReply)
			result := rf.sendRequestVoteWithTimeOut(tmpIndex, voteArgs, voteReply, rf.getElectionTimeOut())
			if result {
				voteResult <- voteReply
			} else {
				voteResult <- nil
			}
		}()

	}

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
	costTime := GetNowMilliTime() - beginTime
	DPrintf("term=%d,role=%s,rf=%d 选举结果收集succ=%d,networkError=%d,耗时：%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, succ, networkError, costTime)
	//Q:如何判定在收集候选者期间，已经有leader联系自己了
	//A:存在leader的情况下，选举必然失败，那么可以通过leader的心跳感知，然后退回follower
	if succ >= majority {
		rf.becomeLeader()
		rf.initNextIndex()
		//		DPrintf("term=%d,role=%s,rf=%d 成为leader,耗时%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, costTime)
		BPrintf("term=%d,role=%s,rf=%d 成为leader,耗时%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, costTime)
		go rf.maintainLeader()

	} else {
		if biggerTermFlag {
			DPrintf("term=%d,role=%s,rf=%d 候选者检测到了更高的term，退化为follower，耗时%d", rf.getRaftTerm(), getRole(rf.getRaftRole()),
				rf.getRaftTerm(), costTime)
			rf.becomeFollower(NONE)
			//			rf.setLastLeaderHeartBeatTime()
		} else {
			//			rf.setVotedFor(NONE)
			DPrintf("term=%d,role=%s,rf=%d 竞选leader失败,succ=%d,networkError=%d,耗时%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, succ, networkError, costTime)
		}
	}

}

//Q leader在什么情况下，会退化为follower。也就是维持leader地位的协程何时停止
//A 发送心跳发现有比自己term更高的时候；有人向leader发起投票请求，然后leader感知到了更高的term；有leader给自己发送心跳请求的时候
func (rf *Raft) maintainLeader() {
	rf.maitainLeaderLock.Lock()
	defer rf.maitainLeaderLock.Unlock()
	for !rf.getRaftIsShutdown() {
		if rf.getRaftRole() != LEADER {
			DPrintf("term=%d,rf=%d 监测到当前角色为已经不是leader，退出leader心跳循环", rf.getRaftTerm(), rf.me)
			return
		}
		req := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}
		begin := GetNowMilliTime()
		req.LeaderCommit = rf.getCommitIndex()
		req.Term = rf.getRaftTerm()
		req.LeaderId = rf.me
		req.Me = rf.me

		currentTerm := req.Term
		n := len(rf.peers)
		chnResult := make(chan *AppendEntriesReply, n)
		DPrintf("term=%d,role=%s,rf=%d 开始发送leader心跳", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me)
		majority := (len(rf.peers) + 1) / 2
		for index := range rf.peers {
			tmpIndex := index
			go func() {
				timeoutChn := make(chan int)
				appendEntryResultChn := make(chan bool)
				go func() {
					time.Sleep(time.Duration(rf.raftHeartBeatIntervalMilli) * time.Millisecond)
					if timeoutChn != nil {
						timeoutChn <- 1
					}
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
			}()

		}
		tmp := 0

		//当大多数的心跳和网络请求失败，那么需要考虑当前节点退回follower的情况
		succ := 0
		networkError := 0
		//Q当发生网络异常的时候，心跳请求可能无法返回，所以需要对请求附加超时机制?
		//A 是的，必须有超时机制，或者说大部分返回就直接进行下一步，否则会影响下次心跳的产生，从而使follower认为leader挂了
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
				break
			}
			if networkError >= majority || succ >= majority || tmp == n {
				break
			}
		}
		if biggerTermFlag {
			rf.becomeFollower(NONE)
			BPrintf("term=%d,role=%s,rf=%d 检测到了更大的term，由leader退回follower，发起心跳时的term=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, currentTerm)
			return
		}
		if networkError == n {
			rf.becomeFollower(NONE)
			BPrintf("term=%d,role=%s,rf=%d 当前节点网络超时，由leader退回follower,耗时=%d，失联时的term为=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, GetNowMilliTime()-begin, currentTerm)
			return
		}
		DPrintf("term=%d,role=%s,rf=%d leader心跳已完成,succ=%d,networkError=%d，发起心跳时的term=%d,耗时=%d", rf.getRaftTerm(), getRole(rf.getRaftRole()), rf.me, succ, networkError, currentTerm, GetNowMilliTime()-begin)
		time.Sleep(time.Duration(rf.raftHeartBeatIntervalMilli) * time.Millisecond)
	}
}

//candidate -> follower
//leader -> follower
func (rf *Raft) becomeFollower(candidateId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = candidateId
	rf.nodeRole = FOLLOWER
	rf.electionTimeoutMS = produceElectionTimeoutParam()
	rf.lastLeaderHeartBeatTime = GetNowMilliTime()
}

func (rf *Raft) becomeCandicate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nodeRole = CANDIDATE
	rf.votedFor = NONE
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nodeRole = LEADER
	rf.votedFor = rf.me
}

func (rf *Raft) initParam() {
	rf.setRaftTerm(0)
	rf.becomeFollower(NONE)
	rf.raftHeartBeatIntervalMilli = 100
	rf.setLastLeaderHeartBeatTime()
	rf.leaderHeartCheckerSwitch = false
	rf.setRaftIsShutdown(false)

	var initEntry LogEntry
	initEntry.Term = 0
	initEntry.Index = 0
	initEntry.Command = -9999

	rf.logEntryLock.Lock()
	rf.logEntries = append(rf.logEntries, initEntry)
	rf.logEntryLock.Unlock()

	rf.mu.Lock()
	rf.currentIndex = -1
	rf.mu.Unlock()

	rf.updateCommitIndex(0)
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
