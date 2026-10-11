package raft

import (
	"testing"
	"time"
)

func appendEntries(r *Raft, term, leaderID int) AppendEntriesReply {
	args := AppendEntriesArgs{Term: term, LeaderID: leaderID}
	return r.Handle("AppendEntries", args).(AppendEntriesReply)
}

func setDeadline(r *Raft, d time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.electionDeadline = d
}

func getDeadline(r *Raft) time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.electionDeadline
}

func TestHeartbeatLowerTermRejected(t *testing.T) {
	v := newVoter()
	v.mu.Lock()
	v.currentTerm = 3
	v.mu.Unlock()
	old := time.Now().Add(-time.Hour)
	setDeadline(v, old)

	reply := appendEntries(v, 2, 0)

	term, _, _ := peek(v)
	if reply.Term != 3 || term != 3 {
		t.Fatalf("reply.Term=%d term=%d, want both 3", reply.Term, term)
	}
	if !getDeadline(v).Equal(old) {
		t.Fatal("a stale heartbeat must not reset the election timer")
	}
}

func TestHeartbeatResetsTimer(t *testing.T) {
	for _, term := range []int{0, 1} {
		v := newVoter()
		setDeadline(v, time.Now().Add(-time.Hour))

		reply := appendEntries(v, term, 0)

		if reply.Term != term {
			t.Fatalf("term %d: reply.Term=%d, want %d", term, reply.Term, term)
		}
		if !getDeadline(v).After(time.Now()) {
			t.Fatalf("term %d: election timer was not reset", term)
		}
	}
}

func TestHeartbeatCandidateStepsDown(t *testing.T) {
	v := newVoter()
	v.mu.Lock()
	v.state, v.currentTerm, v.votedFor = candidate, 3, v.id
	v.mu.Unlock()

	appendEntries(v, 3, 0)

	term, _, st := peek(v)
	if st != follower || term != 3 {
		t.Fatalf("state=%d term=%d, want follower in term 3", st, term)
	}
}

func TestHeartbeatHigherTermAdopted(t *testing.T) {
	v := newVoter()
	v.mu.Lock()
	v.currentTerm, v.votedFor = 1, 2
	v.mu.Unlock()

	appendEntries(v, 2, 0)

	term, votedFor, st := peek(v)
	if term != 2 || votedFor != noVote || st != follower {
		t.Fatalf("term=%d votedFor=%d state=%d, want term 2, no vote, follower", term, votedFor, st)
	}
}

func TestHeartbeatSameTermKeepsVote(t *testing.T) {
	v := newVoter()
	requestVote(v, 1, 0)

	appendEntries(v, 1, 0)

	_, votedFor, _ := peek(v)
	if votedFor != 0 {
		t.Fatalf("votedFor=%d, want 0 (same-term heartbeat must keep the vote)", votedFor)
	}
}

func TestAppendEntriesSuccessOnMatchingPrev(t *testing.T) {
	r := newVoter() // use the same setup line as your TestHeartbeat* tests

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}

	if !reply.Success {
		t.Fatalf("expected Success=true for PrevLogIndex 0 / PrevLogTerm 0 on an empty log, got false (reply term %v)", reply.Term)
	}
}

func TestAppendEntriesRejectsMissingPrev(t *testing.T) {
	r := newVoter()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 5,
		PrevLogTerm:  1,
	}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}

	if reply.Success {
		t.Fatalf("expected Success=false for PrevLogIndex 5 / PrevLogTerm 1 on an empty log, got true (reply term %v)", reply.Term)
	}
}

func TestAppendEntriesStaleTermFails(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 5
	r.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         3,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}

	if reply.Success {
		t.Fatalf("expected Success=false for Term 3 on an empty log, got true (reply term %v)", reply.Term)
	}

	if reply.Term != 5 {
		t.Fatalf("expected reply term to be 5, got %v instead", reply.Term)
	}
}

func TestAppendEntriesSuccessWithLongerLog(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
	}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}

	if !reply.Success {
		t.Fatalf("expected Success=true for PrevLogIndex 1 / PrevLogTerm 1, got false (reply term %v)", reply.Term)
	}
}

func TestAppendEntriesAppendsNewEntries(t *testing.T) {
	r := newVoter()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 1, Command: "a"}, {Term: 1, Command: "b"}},
	}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}

	if !reply.Success {
		t.Fatalf("expected Success=true for PrevLogIndex 0 / PrevLogTerm 0, got false (reply term %v)", reply.Term)
	}

	if r.log.LastIndex() != 2 {
		t.Fatalf("expected to have last index 2 after rpc, has last index %v instead", r.log.LastIndex())
	}

	if r.log.TermAt(2) != 1 {
		t.Fatalf("expected LogEntry number 2 to have term 1, has term %v instead", r.log.TermAt(2))
	}
}

func TestAppendEntriesKeepsMatchingSuffix(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 1}},
	}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}

	if !reply.Success {
		t.Fatalf("expected Success=true for PrevLogIndex 0 / PrevLogTerm 0, got false (reply term %v)", reply.Term)
	}

	if r.log.LastIndex() != 3 {
		t.Fatalf("expected to have last index 3 after rpc, has last index %v instead", r.log.LastIndex())
	}

	if r.log.TermAt(2) != 1 {
		t.Fatalf("expected LogEntry number 2 to have term 1, has term %v instead", r.log.TermAt(2))
	}
}

func TestAppendEntriesTruncatesConflict(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         2,
		LeaderID:     0,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2}, {Term: 2}},
	}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}

	if !reply.Success {
		t.Fatalf("expected Success=true for PrevLogIndex 0 / PrevLogTerm 0, got false (reply term %v)", reply.Term)
	}

	if r.log.LastIndex() != 3 {
		t.Fatalf("expected to have last index 3 after rpc, has last index %v instead", r.log.LastIndex())
	}

	if r.log.TermAt(2) != 2 {
		t.Fatalf("expected LogEntry number 2 to have term 1, has term %v instead", r.log.TermAt(2))
	}
}

func TestHeartbeatDoesNotTruncate(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.mu.Unlock()

	args := AppendEntriesArgs{Term: 1, LeaderID: 0, PrevLogIndex: 0, PrevLogTerm: 0}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}
	if !reply.Success {
		t.Fatalf("expected Success=true, got false (reply term %v)", reply.Term)
	}
	if r.log.LastIndex() != 3 {
		t.Fatalf("heartbeat changed the log: last index is %v instead of 3", r.log.LastIndex())
	}
}

func TestAppendEntriesSkipsMatchingThenAppends(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         1,
		LeaderID:     0,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}, {Term: 1}},
	}

	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}
	if !reply.Success {
		t.Fatalf("expected Success=true, got false (reply term %v)", reply.Term)
	}
	if r.log.LastIndex() != 4 {
		t.Fatalf("expected last index 4 after rpc, got %v", r.log.LastIndex())
	}
}

func TestAppendEntriesCommitsOnlyConfirmedPrefix(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.mu.Unlock()

	args := AppendEntriesArgs{
		Term: 1, LeaderID: 0,
		PrevLogIndex: 1, PrevLogTerm: 1,
		LeaderCommit: 3,
	}
	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok || !reply.Success {
		t.Fatalf("expected a successful reply, got %+v", reply)
	}
	if r.commitIndex != 1 {
		t.Fatalf("commitIndex is %v, expected 1 (only index 1 was confirmed)", r.commitIndex)
	}
}

func TestAppendEntriesCommitCappedByLeaderCommit(t *testing.T) {
	r := newVoter()

	args := AppendEntriesArgs{
		Term: 1, LeaderID: 0,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []LogEntry{{Term: 1}, {Term: 1}, {Term: 1}},
		LeaderCommit: 2,
	}
	r.Handle("AppendEntries", args)

	if r.commitIndex != 2 {
		t.Fatalf("commitIndex is %v, expected 2 (the leader's commit, not the log's end)", r.commitIndex)
	}
}

func TestAppendEntriesFailedPrevCheckKeepsCommitIndex(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.commitIndex = 1
	r.mu.Unlock()

	// My entry at index 2 has term 1, the leader claims term 2: the prev check must fail.
	args := AppendEntriesArgs{
		Term: 1, LeaderID: 0,
		PrevLogIndex: 2, PrevLogTerm: 2,
		LeaderCommit: 3,
	}
	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok {
		t.Fatal("Handle did not return an AppendEntriesReply value")
	}
	if reply.Success {
		t.Fatal("expected Success=false for a term mismatch at PrevLogIndex 2")
	}
	if r.commitIndex != 1 {
		t.Fatalf("failed prev check changed commitIndex to %v, expected 1", r.commitIndex)
	}
}

func TestAppendEntriesLowerLeaderCommitDoesNotLowerCommitIndex(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.commitIndex = 2
	r.mu.Unlock()

	// A delayed RPC: the prev check passes (confirmed through 3), but its LeaderCommit is stale.
	args := AppendEntriesArgs{
		Term: 1, LeaderID: 0,
		PrevLogIndex: 3, PrevLogTerm: 1,
		LeaderCommit: 1,
	}
	reply, ok := r.Handle("AppendEntries", args).(AppendEntriesReply)
	if !ok || !reply.Success {
		t.Fatalf("expected a successful reply, got %+v", reply)
	}
	if r.commitIndex != 2 {
		t.Fatalf("stale LeaderCommit moved commitIndex to %v, expected 2", r.commitIndex)
	}
}

func TestVoteDeniedWhenCandidateLastTermLower(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 3
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 3})
	r.mu.Unlock()

	// the candidate's log is longer, but its last term is older
	args := RequestVoteArgs{Term: 4, CandidateID: 1, LastLogIndex: 4, LastLogTerm: 1}
	reply, ok := r.Handle("RequestVote", args).(RequestVoteReply)
	if !ok {
		t.Fatal("Handle did not return a RequestVoteReply value")
	}
	if reply.VoteGranted {
		t.Fatal("granted a vote to a candidate whose last term is lower")
	}
	if reply.Term != 4 {
		t.Fatalf("reply term is %v, expected 4 (higher term is still adopted)", reply.Term)
	}
}

func TestVoteUpToDateRule(t *testing.T) {
	// Voter's log is [1, 3] (last index 2, last term 3).
	cases := []struct {
		name         string
		lastLogTerm  int
		lastLogIndex int
		wantGranted  bool
	}{
		{"same last term, shorter log", 3, 1, false},
		{"same last term, same length", 3, 2, true},
		{"same last term, longer log", 3, 5, true},
		{"higher last term, shorter log", 4, 1, true},
		{"lower last term, longer log", 1, 9, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newVoter()
			r.mu.Lock()
			r.currentTerm = 3
			r.log.Append(LogEntry{Term: 1})
			r.log.Append(LogEntry{Term: 3})
			r.mu.Unlock()

			args := RequestVoteArgs{
				Term: 4, CandidateID: 1,
				LastLogIndex: tc.lastLogIndex, LastLogTerm: tc.lastLogTerm,
			}
			reply, ok := r.Handle("RequestVote", args).(RequestVoteReply)
			if !ok {
				t.Fatal("Handle did not return a RequestVoteReply value")
			}
			if reply.VoteGranted != tc.wantGranted {
				t.Fatalf("VoteGranted is %v, expected %v", reply.VoteGranted, tc.wantGranted)
			}
			if reply.Term != 4 {
				t.Fatalf("reply term is %v, expected 4", reply.Term)
			}
		})
	}
}

func TestDeniedVoteDoesNotResetElectionTimer(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 3
	r.log.Append(LogEntry{Term: 3})
	past := time.Now().Add(-time.Second)
	r.electionDeadline = past
	r.mu.Unlock()

	// higher term, but the candidate's log is stale, so the vote is denied
	args := RequestVoteArgs{Term: 4, CandidateID: 1, LastLogIndex: 1, LastLogTerm: 1}
	reply := r.Handle("RequestVote", args).(RequestVoteReply)
	if reply.VoteGranted {
		t.Fatal("expected the vote to be denied")
	}

	r.mu.Lock()
	deadline := r.electionDeadline
	r.mu.Unlock()
	if !deadline.Equal(past) {
		t.Fatal("a denied vote postponed my election timer")
	}
}

func TestGrantedVoteResetsElectionTimer(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	past := time.Now().Add(-time.Second)
	r.electionDeadline = past
	r.mu.Unlock()

	args := RequestVoteArgs{Term: 1, CandidateID: 1}
	reply := r.Handle("RequestVote", args).(RequestVoteReply)
	if !reply.VoteGranted {
		t.Fatal("expected the vote to be granted")
	}

	r.mu.Lock()
	deadline := r.electionDeadline
	r.mu.Unlock()
	if !deadline.After(time.Now()) {
		t.Fatal("a granted vote did not reset the election timer")
	}
}

func TestElectionWithNonEmptyLogs(t *testing.T) {
	c := makeCluster(t, 3)
	for _, n := range c.nodes {
		n.mu.Lock()
		n.log.Append(LogEntry{Term: 1})
		n.log.Append(LogEntry{Term: 1})
		n.mu.Unlock()
	}
	c.start()
	c.checkOneLeader(t)
}

func TestStaleLogNodeCannotWin(t *testing.T) {
	c := makeCluster(t, 3)
	// nodes 0 and 1 hold a term-2 entry; node 2 does not
	for _, i := range []int{0, 1} {
		c.nodes[i].mu.Lock()
		c.nodes[i].currentTerm = 2
		c.nodes[i].log.Append(LogEntry{Term: 2})
		c.nodes[i].mu.Unlock()
	}
	c.start()
	leaderID := c.checkOneLeader(t)
	if leaderID == 2 {
		t.Fatal("a node with a stale log won the election")
	}
}
