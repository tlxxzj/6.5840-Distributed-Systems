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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// interval for sending heartbeats to followers
const HeartbeatInterval = 200 * time.Millisecond

// timeout for receiving heartbeats from leader
const HeartbeatTimeout = 2 * HeartbeatInterval

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	nServer  int        // number of servers in the cluster
	role     ServerRole // current role of the server, default is Follower
	term     int        // current term
	votedFor int        // candidateId that received vote in current term, or -1 if none

	lastHeartbeatTime time.Time // last time received heartbeat from leader

	goFuncWaitGroup sync.WaitGroup
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.term, rf.role == Leader
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
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

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = Dead
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// return current state of the server
func (rf *Raft) getRole() ServerRole {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.role
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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).

	rf.nServer = len(rf.peers)
	rf.role = Follower // start as a follower
	rf.term = 0        // start at term 0
	rf.votedFor = -1   // no vote received yet

	rf.lastHeartbeatTime = time.Now()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	//go rf.ticker()
	go rf.runForever()

	return rf
}

// -----------------------

func (rf *Raft) goFunc(f func()) {
	rf.goFuncWaitGroup.Add(1)
	go func() {
		defer rf.goFuncWaitGroup.Done()
		f()
	}()
}

func (rf *Raft) runForever() {
	for {
		switch rf.getRole() {
		case Follower:
			rf.runFollower()
		case Candidate:
			rf.runCandidate()
		case Leader:
			rf.runLeader()
		case Dead:
			rf.goFuncWaitGroup.Wait()
			return
		}
	}
}

func (rf *Raft) runFollower() {
	DPrintf("server %d run as follower", rf.me)

	// reset lastHeartbeatTime
	rf.mu.Lock()
	rf.lastHeartbeatTime = time.Now()
	rf.mu.Unlock()

	for rf.getRole() == Follower {
		// pause for a random amount of time between 50ms and 350ms
		ms := 50 + rand.Int63n(300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		// convert to candidate if no heartbeat received from leader within HeartbeatTimeout
		rf.mu.Lock()
		now := time.Now()

		if now.Sub(rf.lastHeartbeatTime) > HeartbeatTimeout {
			if rf.role == Follower {
				rf.role = Candidate
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) runCandidate() {
	DPrintf("server %d run as candidate", rf.me)

	rf.mu.Lock()
	rf.term++
	rf.votedFor = rf.me

	// construct RequestVoteArgs
	requestArgs := RequestVoteArgs{
		Term:        rf.term,
		CandidateId: rf.me,
	}

	rf.mu.Unlock()

	voteResultCh := make(chan bool, rf.nServer)

	// send RequestVote RPCs to all other servers
	for i := 0; i < rf.nServer; i++ {
		if i == rf.me {
			continue
		}

		server := i
		f := func() {
			args := requestArgs
			reply := RequestVoteReply{}

			ok := false
			for !ok && rf.getRole() == Candidate {
				ok = rf.sendRequestVote(server, &args, &reply)
				if !ok {
					time.Sleep(HeartbeatInterval)
				}
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// return if not candidate anymore
			if rf.role != Candidate {
				return
			}

			// set currentTerm = term if term > currentTerm, convert to follower
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				rf.lastHeartbeatTime = time.Now()

				voteResultCh <- false
				return
			}

			voteResultCh <- reply.VoteGranted
		}

		rf.goFunc(f)

	}

	// wait for votes from other servers
	majority := rf.nServer/2 + 1
	nWin := 1
	timeout := time.After(HeartbeatTimeout)
	isTimeout := false
	for !isTimeout && rf.getRole() == Candidate && nWin < majority {
		select {
		case voteGranted := <-voteResultCh:
			if voteGranted {
				nWin++
			}
		case <-timeout:
			isTimeout = true
		}
	}

	// convert to leader if received majority votes, otherwise convert to follower
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == Candidate && nWin >= majority {
		rf.role = Leader
	} else {
		rf.role = Follower
	}
}

func (rf *Raft) runLeader() {
	DPrintf("server %d run as leader", rf.me)

	wg := sync.WaitGroup{}

	for i := 0; i < rf.nServer; i++ {
		if i == rf.me {
			continue
		}

		wg.Add(1)
		server := i
		f := func() {
			defer wg.Done()
			rf.runFollowerWorker(server)
		}
		rf.goFunc(f)
	}

	// wait for all workers to finish
	wg.Wait()
}

// send AppendEntries RPCs to followers
func (rf *Raft) runFollowerWorker(server int) {
	for rf.getRole() == Leader {
		rf.mu.RLock()
		args := AppendEntriesArgs{
			Term:     rf.term,
			LeaderId: rf.me,
		}
		rf.mu.RUnlock()

		reply := AppendEntriesReply{}

		ok := false
		for !ok && rf.getRole() == Leader {
			ok = rf.sendAppendEntries(server, &args, &reply)
			if !ok {
				time.Sleep(HeartbeatInterval)
			}
		}

		rf.mu.Lock()

		// return if not leader anymore
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		// set currentTerm = term if term > currentTerm, convert to follower
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.role = Follower
			rf.votedFor = -1
			rf.lastHeartbeatTime = time.Now()
		}

		rf.mu.Unlock()

		time.Sleep(HeartbeatInterval)
	}
}
