package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"time"

	//	"6.5840/labgob"
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
	CandidateId int
	Term        int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if (args.Term == rf.term && (rf.voteFor == -1 || rf.voteFor == args.CandidateId)) || (rf.term < args.Term) {
		rf.term = args.Term
		rf.voteFor = args.CandidateId
		rf.state = Follower
		rf.lastContact = time.Now()
		reply.Term = rf.term
		reply.VoteGranted = true
		rf.electionTimeout = randomElectionTimeout()
		return
	}
	// liang zhong qing kuang , yi zhong shi ci shi rf.terms > args.term
	// ling yi zhong jiushi rf.term == args.term  dan shi voteFor != -1
	reply.Term = rf.term
	reply.VoteGranted = false

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// zhe li zhu yao shi xiang dnag yu fa song xin tiao ran hou geng xin ci shi ta men de shang yi
	//ci xin tiao de zhe ge shi jian
	reply.Term = rf.term
	reply.Success = false

	if rf.term > args.Term {
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.voteFor = -1
	}

	rf.state = Follower
	rf.lastContact = time.Now()
	rf.electionTimeout = randomElectionTimeout()

	reply.Term = rf.term
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(300)) * time.Millisecond
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
	votes := 1
	electionTerm := rf.term
	majority := len(rf.peers)/2 + 1

	// xiu gai wan le zhuang tai zhi hou chuang jian ci shi de args he reply zhi hou fa song
	args := RequestVoteArgs{
		Term:        rf.term,
		CandidateId: rf.me,
	}

	if votes >= majority {
		rf.state = Leader
		rf.lastHeartbeat = time.Now()
		rf.mu.Unlock()
		rf.sendHeartbeats()
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
					rf.state = Leader
					rf.lastHeartbeat = time.Now()
					becameLeader = true
				}
			}

			rf.mu.Unlock()

			if becameLeader {
				rf.sendHeartbeats()
			}
		}(server)
	}
	// ran hou gen ju fa song de qing kuang jin xing chu li
}

func (rf *Raft) sendHeartbeats() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.term
	rf.lastHeartbeat = time.Now()
	args := AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
	}

	rf.mu.Unlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}

		go func(server int) {
			reply := AppendEntriesReply{}

			ok := rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.state = Follower
				rf.voteFor = -1
				rf.lastContact = time.Now()
				rf.electionTimeout = randomElectionTimeout()
			}
		}(server)
	}
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
			rf.sendHeartbeats()
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
	rf.lastContact = time.Now()
	rf.lastHeartbeat = time.Now()
	rf.state = Follower
	rf.term = 0
	rf.voteFor = -1
	rf.heartbeatInterval = 150 * time.Millisecond
	rf.electionTimeout = randomElectionTimeout()

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
