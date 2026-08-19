package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type StateType int

const (
	Idle StateType = iota
	Follower
	Leader
	Candidate
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type PersistentState struct {
	Term    int
	VoteFor int
	Log     []LogEntry
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                sync.Mutex          // Lock to protect shared access to this peer's state
	peers             []*labrpc.ClientEnd // RPC end points of all peers
	persister         *tester.Persister   // Object to hold this peer's persisted state
	me                int                 // this peer's index into peers[]
	state             StateType
	lastContact       time.Time
	lastHeartbeat     time.Time
	electionTimeout   time.Duration
	heartbeatInterval time.Duration
	term              int
	voteFor           int
	log               []LogEntry
	nextIndex         []int
	matchIndex        []int

	commitIndex int
	lastApplied int

	applyCh   chan raftapi.ApplyMsg
	applyCond *sync.Cond
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// zhe li wo hai xu yao jia zai shen me lei xing de shu ju jie gou
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (3A).
	// zhe li fan hui de zhuang tai shi ci shi pan duan wo shi follower hai shi
	// leader ma ?  term shi shen me lai zhe jiu shi ci shi
	// shi di ji ci xuan ju shi ma ?
	// ru guo you hen duo ge raft na me ci shi mei yi ge raft dou yao shi xina yige term
	term = rf.term
	isleader = (rf.state == Leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	state := PersistentState{
		Term:    rf.term,
		VoteFor: rf.voteFor,
		Log:     rf.log,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(state) != nil {
		return
	}

	rf.persister.Save(w.Bytes(), nil)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var state PersistentState
	if d.Decode(&state) != nil {
		return
	}
	rf.term = state.Term
	rf.log = state.Log
	rf.voteFor = state.VoteFor

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	CandidateId  int
	Term         int
	LastlogTerm  int
	LastlogIndex int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevlogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
	XLen    int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false

	if args.Term < rf.term {
		return
	}
	persistentChanged := false
	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
		rf.state = Follower
		persistentChanged = true
	}

	reply.Term = rf.term

	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term

	upToDate := args.LastlogTerm > myLastTerm ||
		(args.LastlogTerm == myLastTerm && args.LastlogIndex >= myLastIndex)

	canVote := rf.voteFor == -1 || rf.voteFor == args.CandidateId

	if upToDate && canVote {
		rf.voteFor = args.CandidateId
		rf.state = Follower
		rf.lastContact = time.Now()
		rf.electionTimeout = randomElectionTimeout()
		reply.VoteGranted = true
		persistentChanged = true
	}

	if persistentChanged {
		rf.persist()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	persistentChanged := false
	defer func() {
		if persistentChanged {
			rf.persist()
		}
		rf.mu.Unlock()
	}()

	reply.Term = rf.term
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = len(rf.log)
	if args.Term < rf.term {
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
		persistentChanged = true
	}

	rf.state = Follower
	rf.lastContact = time.Now()
	rf.electionTimeout = randomElectionTimeout()
	reply.Term = rf.term

	if args.PrevLogIndex >= len(rf.log) {
		reply.Term = -1
		reply.XLen = len(rf.log)
		return
	}

	if rf.log[args.PrevLogIndex].Term != args.PrevlogTerm {
		conflictTerm := rf.log[args.PrevLogIndex].Term
		firstIndex := args.PrevLogIndex

		for firstIndex > 0 && rf.log[firstIndex-1].Term == conflictTerm {
			firstIndex--
		}

		reply.XTerm = conflictTerm
		reply.XIndex = firstIndex
		reply.XLen = len(rf.log)
		return
	}

	insertIndex := args.PrevLogIndex + 1
	entryIndex := 0
	for entryIndex < len(args.Entries) && insertIndex+entryIndex < len(rf.log) {
		localIndex := insertIndex + entryIndex
		if rf.log[localIndex].Term != args.Entries[entryIndex].Term {
			rf.log = rf.log[:localIndex]
			persistentChanged = true
			break
		}
		entryIndex++
	}

	// zhe li jiu ke yi cong entryIndex ba zhe xie dong xi cha ru jin qu le
	if entryIndex < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[entryIndex:]...)
		persistentChanged = true
	}

	if args.LeaderCommit > rf.commitIndex {
		lastLogIndex := len(rf.log) - 1
		newCommitIndex := args.LeaderCommit

		if newCommitIndex > lastLogIndex {
			newCommitIndex = lastLogIndex
		}

		if newCommitIndex > rf.commitIndex {
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}

	}

	reply.Success = true

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) advanceCommitIndexLocked() {
	for index := len(rf.log) - 1; index > rf.commitIndex; index-- {
		if rf.log[index].Term != rf.term {
			continue
		}
		count := 0
		for server := range rf.peers {
			if rf.matchIndex[server] >= index {
				count++
			}
		}
		if count >= len(rf.peers)/2+1 {
			rf.commitIndex = index

			rf.applyCond.Signal()
			break
		}
	}
}

func (rf *Raft) replicateToPeer(server int) {
	// zhe li shou xian yao pan duan ci shi shi bu shi leader
	//ru guo bu shi jiu zhi jie fan hui
	//ru guo shi de hua ci shi jiu ji xue zhi xing gou zao ci shi de args he dui ying de reply
	for {
		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}

		term := rf.term
		next := rf.nextIndex[server]
		prevIndex := next - 1

		entries := append([]LogEntry(nil), rf.log[next:]...)
		args := AppendEntriesArgs{
			Term:         term,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevlogTerm:  rf.log[prevIndex].Term,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}

		ok := rf.sendAppendEntries(server, &args, &reply)

		if !ok {
			return
		}
		rf.mu.Lock()

		if reply.Term > rf.term {
			rf.state = Follower
			rf.term = reply.Term
			rf.voteFor = -1
			rf.lastContact = time.Now()
			rf.electionTimeout = randomElectionTimeout()
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if rf.state != Leader || rf.term != term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			matched := args.PrevLogIndex + len(args.Entries)
			if matched > rf.matchIndex[server] {
				rf.matchIndex[server] = matched
				rf.nextIndex[server] = matched + 1
			}

			rf.advanceCommitIndexLocked()
			rf.mu.Unlock()
			return
		} else {
			newNext := reply.XLen

			if reply.XTerm != -1 {
				lastIndexWithTerm := -1
				for index := len(rf.log) - 1; index >= 0; index-- {
					if rf.log[index].Term == reply.Term {
						lastIndexWithTerm = index
						break
					}
				}

				if lastIndexWithTerm != -1 {
					newNext = lastIndexWithTerm + 1
				} else {
					newNext = reply.XIndex
				}
			}
			if newNext < 1 {
				newNext = 1
			}

			if newNext > len(rf.log) {
				newNext = len(rf.log)
			}

			minNext := rf.matchIndex[server] + 1
			if newNext < minNext {
				newNext = minNext
			}

			if rf.nextIndex[server] == next {
				rf.nextIndex[server] = newNext
			}

			rf.mu.Unlock()
			continue
		}

		rf.mu.Unlock()

	}

}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go rf.replicateToPeer(server)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	term := rf.term

	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, term, false
	}

	entry := LogEntry{
		Term:    term,
		Command: command,
	}
	// ran hou ci shi ba xian zai de entry append dao log slice zhong
	rf.log = append(rf.log, entry)
	// geng xin nextIndex
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	// geng xin matchIndex

	// broadecast
	rf.persist()
	rf.mu.Unlock()
	go rf.broadcastAppendEntries()
	return index, term, true
	// return

}

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) becomeLeaderLocked() {
	rf.state = Leader

	lastIndex := len(rf.log) - 1

	for server := range rf.peers {
		rf.nextIndex[server] = lastIndex + 1
		rf.matchIndex[server] = 0
	}

	rf.nextIndex[rf.me] = lastIndex + 1
	rf.matchIndex[rf.me] = lastIndex
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) startElection() {
	// zhe li xian ba xuan ju de luo ji xie hao
	rf.mu.Lock()
	if rf.state == Leader ||
		time.Since(rf.lastContact) < rf.electionTimeout {
		rf.mu.Unlock()
		return
	}

	// ci shi ke xuan ju le shou xian xiu gai ci shi rf de zhuang tai
	rf.state = Candidate
	rf.term++
	rf.voteFor = rf.me
	rf.electionTimeout = randomElectionTimeout()
	rf.lastContact = time.Now()

	rf.persist()
	votes := 1
	electionTerm := rf.term
	majority := len(rf.peers)/2 + 1
	lastlogIndex := len(rf.log) - 1
	lastlogTerm := rf.log[lastlogIndex].Term

	// xiu gai wan le zhuang tai zhi hou chuang jian ci shi de args he reply zhi hou fa song
	args := RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastlogTerm:  lastlogTerm,
		LastlogIndex: lastlogIndex,
	}

	if votes >= majority {
		rf.becomeLeaderLocked()
		rf.mu.Unlock()
		rf.broadcastAppendEntries()
		return
	}

	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			becameLeader := false
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				return
			}

			rf.mu.Lock()

			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.state = Follower
				rf.voteFor = -1
				rf.lastContact = time.Now()
				rf.electionTimeout = randomElectionTimeout()
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if rf.state != Candidate ||
				rf.term != electionTerm ||
				reply.Term != electionTerm {
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted {
				votes++
				if votes >= len(rf.peers)/2+1 {
					rf.becomeLeaderLocked()
					rf.lastHeartbeat = time.Now()
					becameLeader = true
				}
			}

			rf.mu.Unlock()

			if becameLeader {
				rf.broadcastAppendEntries()
			}
		}(server)
	}
	// ran hou gen ju fa song de qing kuang jin xing chu li
}

func (rf *Raft) ticker() {
	for true {
		time.Sleep(50 * time.Millisecond)
		now := time.Now()
		rf.mu.Lock()
		needHeartbeat := rf.state == Leader && now.Sub(rf.lastHeartbeat) >= rf.heartbeatInterval
		needElection := rf.state != Leader && now.Sub(rf.lastContact) >= rf.electionTimeout

		rf.mu.Unlock()

		if needElection {
			rf.startElection()
		} else if needHeartbeat {
			rf.broadcastAppendEntries()
		}

	}
}

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}

		start := rf.lastApplied + 1
		end := rf.commitIndex

		entries := append([]LogEntry(nil), rf.log[start:end+1]...)

		rf.lastApplied = end
		rf.mu.Unlock()

		for offset, entry := range entries {
			index := start + offset
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: index,
			}

		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.lastContact = time.Now()
	rf.lastHeartbeat = time.Now()
	rf.state = Follower
	rf.term = 0
	rf.voteFor = -1
	rf.heartbeatInterval = 150 * time.Millisecond
	rf.electionTimeout = randomElectionTimeout()

	// zhe li chu shi hua chu le wen ti , er qie ci shi de applyCh zhe ge channel ye mei you bei yong dao
	rf.log = []LogEntry{
		{
			Term:    0,
			Command: nil,
		},
	}
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	// Your initialization code here (3A, 3B, 3C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
