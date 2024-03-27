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
		rf.persist()
		rf.lastHeartbeatTime = time.Now()
	}

	// if candidate's log is not up-to-date, reject vote
	// "up-to-date" means:
	// 1. if two logs have last entries with different terms, the log with the later term is more up-to-date
	// 2. if two logs end with the same term, the log is longer is more up-to-date
	lastEntry := rf.logStorage.Last()
	if args.LastLogTerm < lastEntry.Term ||
		(args.LastLogTerm == lastEntry.Term && args.LastLogIndex < lastEntry.Index) {
		return
	}

	// if votedFor is -1 or candidateId, grant vote
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			rf.persist()
		}
		reply.VoteGranted = true
		rf.lastHeartbeatTime = time.Now()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("Server %d received AppendEntries from %d", rf.me, args.LeaderId)

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
		rf.persist()
	}

	// prevLogIndex does not exist in log
	lastEntry := rf.logStorage.Last()
	if args.PrevLogIndex > lastEntry.Index {
		reply.ConflictIndex = lastEntry.Index + 1
		reply.ConflictTerm = -1
		return
	}

	// prevLogTerm does not match
	// set conflictTerm to the term of the conflicting entry
	// set conflictIndex to the first index of the conflicting term
	if args.PrevLogTerm != rf.logStorage.Get(args.PrevLogIndex).Term {
		reply.ConflictTerm = rf.logStorage.Get(args.PrevLogIndex).Term
		ConflictEntry := rf.logStorage.FindFirstByTerm(reply.ConflictTerm)
		reply.ConflictIndex = ConflictEntry.Index
		return
	}

	// append new entries
	for i, entry := range args.Entries {
		if entry.Index > rf.logStorage.LastIndex() {
			// append new entries if they are not in the log
			rf.logStorage.Append(args.Entries[i:]...)
			break
		} else if entry.Term != rf.logStorage.Get(entry.Index).Term {
			// delete existing entries that conflict with new entries
			rf.logStorage.DeleteFrom(entry.Index)
			rf.logStorage.Append(args.Entries[i:]...)
			break
		}
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		newCommitIndex := min(args.LeaderCommit, rf.logStorage.LastIndex())
		// Figure 8, only commit entries from current term
		if newCommitIndex > rf.commitIndex && rf.logStorage.Get(newCommitIndex).Term == rf.term {
			rf.commitIndex = newCommitIndex
			rf.goFunc(func() {
				select {
				case rf.triggerApplyCh <- struct{}{}:
				default:
				}
			})
			//DPrintf("Server %d update commitIndex to %d", rf.me, rf.commitIndex)

			rf.persist()
		}
	}

	reply.Success = true
}
