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

func TestBuildAppendEntries(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	r.log.Append(LogEntry{Term: 1, Command: "a"})
	r.log.Append(LogEntry{Term: 1, Command: "b"})
	r.log.Append(LogEntry{Term: 2, Command: "c"})
	r.becomeLeaderLocked()

	peer := 1 // use any peer ID that isn't r.id; check what newVoter sets up

	// peer is behind: nextIndex 2 means "send entries 2 and 3"
	r.nextIndex[peer] = 2
	r.commitIndex = 2
	args := r.buildAppendEntriesLocked(peer)
	if args.Term != 2 || args.LeaderID != r.id {
		t.Fatalf("wrong term/leader in args: %+v", args)
	}
	if args.PrevLogIndex != 1 || args.PrevLogTerm != 1 {
		t.Fatalf("expected prev 1/1, got %v/%v", args.PrevLogIndex, args.PrevLogTerm)
	}
	if len(args.Entries) != 2 || args.Entries[0].Command != "b" {
		t.Fatalf("expected entries [b c], got %+v", args.Entries)
	}

	if args.LeaderCommit != 2 {
		t.Fatalf("expected LeaderCommit 2, got %v", args.LeaderCommit)
	}

	// peer is up to date: heartbeat, no entries
	r.nextIndex[peer] = r.log.LastIndex() + 1
	args = r.buildAppendEntriesLocked(peer)
	if args.PrevLogIndex != 3 || args.PrevLogTerm != 2 || len(args.Entries) != 0 {
		t.Fatalf("expected empty heartbeat with prev 3/2, got %+v", args)
	}

	// peer has nothing: send everything, anchored at the sentinel
	r.nextIndex[peer] = 1
	args = r.buildAppendEntriesLocked(peer)
	if args.PrevLogIndex != 0 || args.PrevLogTerm != 0 || len(args.Entries) != 3 {
		t.Fatalf("expected prev 0/0 with 3 entries, got %+v", args)
	}
}

func TestReplySuccessUsesSentEntries(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	r.log.Append(LogEntry{Term: 1})
	r.log.Append(LogEntry{Term: 2})
	r.log.Append(LogEntry{Term: 2})
	r.becomeLeaderLocked()

	peer := 1
	r.nextIndex[peer] = 2
	args := r.buildAppendEntriesLocked(peer) // prev 1, entries for indices 2 and 3

	// two Submits happen while the RPC is in flight
	r.log.Append(LogEntry{Term: 2})
	r.log.Append(LogEntry{Term: 2})

	r.handleAppendEntriesReplyLocked(peer, args, AppendEntriesReply{Term: 2, Success: true})

	if r.matchIndex[peer] != 3 {
		t.Fatalf("matchIndex is %v, expected 3 (what was sent)", r.matchIndex[peer])
	}
	if r.nextIndex[peer] != 4 {
		t.Fatalf("nextIndex is %v, expected 4", r.nextIndex[peer])
	}
}

func TestReplyFailureDuplicateIsIdempotent(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	for i := 0; i < 5; i++ {
		r.log.Append(LogEntry{Term: 2})
	}
	r.becomeLeaderLocked()
	peer := 1
	r.nextIndex[peer] = 5
	args := r.buildAppendEntriesLocked(peer) // prev 4

	fail := AppendEntriesReply{Term: 2, Success: false}
	r.handleAppendEntriesReplyLocked(peer, args, fail)
	r.handleAppendEntriesReplyLocked(peer, args, fail) // same reply delivered twice

	if r.nextIndex[peer] != 4 {
		t.Fatalf("nextIndex is %v, expected 4", r.nextIndex[peer])
	}

	// an old failure must not move nextIndex forward
	r.nextIndex[peer] = 2
	r.handleAppendEntriesReplyLocked(peer, args, fail)
	if r.nextIndex[peer] != 2 {
		t.Fatalf("stale failure moved nextIndex to %v, expected 2", r.nextIndex[peer])
	}
}

func TestReplyFailureFloorIsOne(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	r.becomeLeaderLocked()
	peer := 1
	r.nextIndex[peer] = 1
	args := r.buildAppendEntriesLocked(peer) // prev 0

	r.handleAppendEntriesReplyLocked(peer, args, AppendEntriesReply{Term: 2, Success: false})
	if r.nextIndex[peer] != 1 {
		t.Fatalf("nextIndex is %v, expected 1", r.nextIndex[peer])
	}
}

func TestReplyDuplicateSuccessDoesNotBackOff(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	for i := 0; i < 3; i++ {
		r.log.Append(LogEntry{Term: 2})
	}
	r.becomeLeaderLocked()
	peer := 1
	r.nextIndex[peer] = 2
	args := r.buildAppendEntriesLocked(peer) // prev 1, entries for 2 and 3

	ok := AppendEntriesReply{Term: 2, Success: true}
	r.handleAppendEntriesReplyLocked(peer, args, ok)
	r.handleAppendEntriesReplyLocked(peer, args, ok) // duplicate delivery

	if r.matchIndex[peer] != 3 || r.nextIndex[peer] != 4 {
		t.Fatalf("after duplicate: matchIndex %v nextIndex %v, expected 3 and 4",
			r.matchIndex[peer], r.nextIndex[peer])
	}

	hb := r.buildAppendEntriesLocked(peer) // prev 3, no entries
	r.handleAppendEntriesReplyLocked(peer, hb, ok)
	if r.nextIndex[peer] != 4 {
		t.Fatalf("heartbeat success moved nextIndex to %v, expected 4", r.nextIndex[peer])
	}
}

func TestAdvanceCommitIndexMajority(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	for i := 0; i < 4; i++ {
		r.log.Append(LogEntry{Term: 2})
	}
	r.becomeLeaderLocked()
	r.matchIndex[1] = 3
	r.matchIndex[2] = 1

	r.advanceCommitIndexLocked()

	// leader has 4, peer 1 has 3, peer 2 has 1: majority (2 of 3) holds up to 3
	if r.commitIndex != 3 {
		t.Fatalf("commitIndex is %v, expected 3", r.commitIndex)
	}
}

func TestAdvanceCommitIndexNeedsMajority(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	r.log.Append(LogEntry{Term: 2})
	r.becomeLeaderLocked() // no follower has anything

	r.advanceCommitIndexLocked()

	if r.commitIndex != 0 {
		t.Fatalf("leader alone committed %v, expected 0", r.commitIndex)
	}
}

func TestAdvanceCommitIndexOnlyCurrentTerm(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	for i := 0; i < 3; i++ {
		r.log.Append(LogEntry{Term: 1}) // entries from an older term
	}
	r.becomeLeaderLocked()
	r.matchIndex[1] = 3 // a majority holds them

	r.advanceCommitIndexLocked()
	if r.commitIndex != 0 {
		t.Fatalf("committed an old-term entry by counting: commitIndex %v, expected 0", r.commitIndex)
	}

	// once an entry from the current term reaches a majority, the older ones commit with it
	r.log.Append(LogEntry{Term: 2})
	r.matchIndex[1] = 4
	r.advanceCommitIndexLocked()
	if r.commitIndex != 4 {
		t.Fatalf("commitIndex is %v, expected 4", r.commitIndex)
	}
}

func TestReplySuccessAdvancesCommit(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	for i := 0; i < 3; i++ {
		r.log.Append(LogEntry{Term: 2})
	}
	r.becomeLeaderLocked()
	peer := 1
	r.nextIndex[peer] = 1
	args := r.buildAppendEntriesLocked(peer) // prev 0, entries 1..3

	r.handleAppendEntriesReplyLocked(peer, args, AppendEntriesReply{Term: 2, Success: true})

	if r.commitIndex != 3 {
		t.Fatalf("commitIndex is %v, expected 3 (leader + peer 1 is a majority of 3)", r.commitIndex)
	}
}

func TestAdvanceCommitIndexNeverDecreases(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.currentTerm = 2
	for i := 0; i < 3; i++ {
		r.log.Append(LogEntry{Term: 1})
	}
	r.becomeLeaderLocked()
	r.matchIndex[1] = 1
	r.commitIndex = 3

	r.advanceCommitIndexLocked()
	if r.commitIndex != 3 {
		t.Fatal("commit index decreased")
	}
}
