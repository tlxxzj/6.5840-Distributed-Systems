package raft

type ServerRole int32

const (
	Follower  ServerRole = iota
	Candidate ServerRole = iota
	Leader    ServerRole = iota
	Dead      ServerRole = iota
)

// arguments to request vote
type RequestVoteArgs struct {
	Term         int // candidate's term
	CandidateId  int // candidate's id
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// reply from request vote
type RequestVoteReply struct {
	Term        int  // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// args to append entries
type AppendEntriesArgs struct {
	Term         int           // leader's term
	LeaderId     int           // leader's id, so follower can redirect clients
	PrevLogIndex int           // index of log entry immediately preceding new ones
	PrevLogTerm  int           // term of prevLogIndex entry
	Entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int           // leader's commitIndex
}

// reply from append entries
type AppendEntriesReply struct {
	Term    int  // current term, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}
