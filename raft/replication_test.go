package raft

import "testing"

func TestSubmitOnFollower(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.mu.Unlock()

	_, _, isLeader := r.Submit("x")

	if isLeader {
		t.Fatal("expected node to be follower")
	}

	if r.log.LastIndex() != 2 || r.log.TermAt(2) != 1 {
		t.Fatal("expected log to remain unchanged")
	}
}

func TestSubmitOnLeader(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.currentTerm = 1
	r.state = leader
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 1})
	r.mu.Unlock()

	_, _, isLeader := r.Submit("x")

	if !isLeader {
		t.Fatal("expected node to be leader")
	}

	if r.log.LastIndex() != 3 || r.log.TermAt(3) != 1 {
		t.Fatal("expected log to change")
	}
}

func TestBecomeLeaderInitializesIndexes(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 2})

	r.becomeLeaderLocked()

	if r.state != leader {
		t.Fatalf("expected state leader, got %v", r.state)
	}
	if len(r.nextIndex) != len(r.peers) || len(r.matchIndex) != len(r.peers) {
		t.Fatalf("expected nextIndex and matchIndex of length %v, got %v and %v",
			len(r.peers), len(r.nextIndex), len(r.matchIndex))
	}
	for p := range r.peers {
		if r.nextIndex[p] != r.log.LastIndex()+1 {
			t.Fatalf("nextIndex[%v] is %v, expected %v", p, r.nextIndex[p], r.log.LastIndex()+1)
		}
		if r.matchIndex[p] != 0 {
			t.Fatalf("matchIndex[%v] is %v, expected 0", p, r.matchIndex[p])
		}
	}
}
