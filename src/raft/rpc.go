package raft

import "time"

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %d received RequestVote from %d", rf.me, args.CandidateId)

	reply.Term = rf.term
	reply.VoteGranted = false

	// reply false if term < currentTerm
	if args.Term < rf.term {
		return
	}

	// set currentTerm = term if term > currentTerm, convert to follower
	if args.Term > rf.term {
		rf.role = Follower
		rf.term = args.Term
		rf.votedFor = -1
		rf.lastHeartbeatTime = time.Now()
	}

	// if votedFor is -1 or candidateId, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server %d received AppendEntries from %d", rf.me, args.LeaderId)

	reply.Term = rf.term
	reply.Success = false

	// reply false if term < currentTerm
	if args.Term < rf.term {
		return
	}

	// update lastHeartbeatTime
	rf.lastHeartbeatTime = time.Now()
	// convert to follower
	rf.role = Follower

	// set currentTerm = term if term > currentTerm, convert to follower
	if args.Term > rf.term {
		rf.role = Follower
		rf.term = args.Term
		rf.votedFor = -1
	}

}
