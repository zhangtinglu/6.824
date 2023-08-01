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

import (
	//	"bytes"

	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

var (
	logger = log.New(os.Stdout, "[raft]", log.LstdFlags|log.Lmicroseconds)
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Role uint32

const (
	Follower Role = iota
	Candidate
	Leader
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor         int
	log              []LogEntry
	currentTerm      uint64
	commitIndex      uint64
	lastApplied      uint64
	role             Role
	HeartbeatTimeout time.Duration
	ElectionTimeout  time.Duration
	rpcCh            chan interface{}
}

type LogEntry struct {
	Term    uint64
	Index   uint64
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(atomic.LoadUint64(&rf.currentTerm))
	isleader = rf.role == Leader
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
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		role := rf.getRole()
		switch role {
		case Leader:
			logger.Printf("[s%d] is leader, currentTerm: %d", rf.me, rf.currentTerm)
			rf.runLeader()
		case Follower:
			logger.Printf("[s%d] is follower, currentTerm: %d", rf.me, rf.currentTerm)
			rf.runFollower()
		case Candidate:
			logger.Printf("[s%d] is candidate, currentTerm: %d", rf.me, rf.currentTerm)
			rf.runCandidate()
		}
	}
}

func (rf *Raft) runFollower() {
	for rf.getRole() == Follower {
		select {
		case <-rf.rpcCh:
			continue
		case <-randomTimeout(rf.ElectionTimeout):
			rf.setRole(Candidate)
			return
		}
	}
}

func (rf *Raft) runLeader() {
	rf.broadcastHearteat()
	for rf.getRole() == Leader {
		select {
		case <-rf.rpcCh:
			continue
		case <-time.After(rf.HeartbeatTimeout):
			rf.broadcastHearteat()
		}
	}
}

func (rf *Raft) runCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me

	voteReceived := 1
	voteCh := make(chan bool, len(rf.peers))

	// send RequestVote RPCs to all other servers concurrently
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: 0,
				LastLogTerm:  0,
			}

			if rf.log != nil {
				args = RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.log[len(rf.log)-1].Index,
					LastLogTerm:  rf.log[len(rf.log)-1].Term,
				}
			}

			reply := RequestVoteReply{}
			ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
			if ok {
				voteCh <- reply.VoteGranted
			}
			logger.Printf("[s%d] Raft.RequestVote -> [s%d]", rf.me, server)
		}(i)
	}

	timer := randomTimeout(rf.ElectionTimeout)
	for rf.getRole() == Candidate {
		select {
		case <-voteCh:
			voteReceived++
			if voteReceived > len(rf.peers)/2 {
				rf.setRole(Leader)
				return
			}
		case <-timer:
			return
		}
	}
}

func (rf *Raft) getRole() Role {
	roleAddr := (*uint32)(&rf.role)
	return Role(atomic.LoadUint32(roleAddr))
}

func (rf *Raft) setRole(role Role) {
	roleAddr := (*uint32)(&rf.role)
	atomic.StoreUint32(roleAddr, uint32(role))
}

func (rf *Raft) broadcastHearteat() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	if rf.log != nil {
		args.PrevLogIndex = rf.log[len(rf.log)-1].Index
		args.PrevLogTerm = rf.log[len(rf.log)-1].Term
	}

	reply := AppendEntriesReply{}
	rf.broadcastAppendEntries(&args, &reply)
}

func (rf *Raft) broadcastAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func(server int) {
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.setRole(Follower)
					rf.currentTerm = reply.Term
					rf.votedFor = -1
				}
			}
			logger.Printf("[s%d] Raft.AppendEntries -> [s%d]", rf.me, server)
		}(i)
	}
}

func randomTimeout(min time.Duration) <-chan time.Time {
	extra := time.Duration(rand.Int63()) % min
	return time.After(min + extra)
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
	rf.role = Follower
	rf.HeartbeatTimeout = 1500 * time.Millisecond
	rf.ElectionTimeout = 2000 * time.Millisecond
	rf.rpcCh = make(chan interface{}, 100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         uint64
	CandidateId  int
	LastLogIndex uint64
	LastLogTerm  uint64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         uint64
	LeaderId     int
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term    uint64
	Success bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.Term > rf.currentTerm {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			if rf.log == nil {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				reply.Term = rf.currentTerm
				logger.Printf("[s%d] voteFor -> [s%d]", rf.me, args.CandidateId)
				return
			}
			if args.LastLogTerm >= rf.log[len(rf.log)-1].Term && args.LastLogIndex >= rf.log[len(rf.log)-1].Index {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				reply.Term = rf.currentTerm
				logger.Printf("[s%d] voteFor -> [s%d]", rf.me, args.CandidateId)
				return
			}
		}

	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
}

// AppendEntries RPC handler.[主库发送心跳]
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		return
	}
	// 不要再接着ticker了
	reply.Success = true
}

func (rf *Raft) SendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
