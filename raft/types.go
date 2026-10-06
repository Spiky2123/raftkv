package raft

import "encoding/gob"

type RequestVoteArgs struct {
	Term        int // The candidate's term
	CandidateID int // ID of the node requesting the vote
}

type RequestVoteReply struct {
	Term        int  // Voter's term
	VoteGranted bool // true if the node voted for the candidate for this term
}

type AppendEntriesArgs struct {
	Term     int // Leader's term
	LeaderID int // Leaders ID
}

type AppendEntriesReply struct {
	Term int // Current term so a stale leader learns it's behind
}

func init() {
	gob.Register(RequestVoteArgs{})
	gob.Register(AppendEntriesArgs{})
	gob.Register(LogEntry{})
}
