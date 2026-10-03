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
