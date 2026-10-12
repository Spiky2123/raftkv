package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

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

func TestCollectApplyMessages(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	defer r.mu.Unlock()

	r.log.Append(LogEntry{Term: 1, Command: "a"})
	r.log.Append(LogEntry{Term: 1, Command: "b"})
	r.log.Append(LogEntry{Term: 1, Command: "c"})

	if msgs := r.collectApplyLocked(); len(msgs) != 0 {
		t.Fatalf("nothing is committed, got %d messages", len(msgs))
	}

	r.commitIndex = 2
	msgs := r.collectApplyLocked()
	if len(msgs) != 2 || msgs[0].Index != 1 || msgs[0].Command != "a" ||
		msgs[1].Index != 2 || msgs[1].Command != "b" {
		t.Fatalf("expected [1:a 2:b], got %+v", msgs)
	}
	if r.lastApplied != 2 {
		t.Fatalf("lastApplied is %v, expected 2", r.lastApplied)
	}

	if msgs := r.collectApplyLocked(); len(msgs) != 0 {
		t.Fatalf("entries delivered twice: %+v", msgs)
	}

	r.commitIndex = 3
	msgs = r.collectApplyLocked()
	if len(msgs) != 1 || msgs[0].Index != 3 || msgs[0].Command != "c" {
		t.Fatalf("expected [3:c], got %+v", msgs)
	}
}

func TestApplierDeliversInOrder(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.log.Append(LogEntry{Term: 1, Command: "a"})
	r.log.Append(LogEntry{Term: 1, Command: "b"})
	r.log.Append(LogEntry{Term: 1, Command: "c"})
	r.commitIndex = 2
	r.mu.Unlock()

	go r.applier()
	defer r.Kill()

	recv := func(wantIndex int, wantCmd string) {
		select {
		case msg := <-r.ApplyCh():
			if msg.Index != wantIndex || msg.Command != wantCmd {
				t.Fatalf("got %+v, expected index %d command %q", msg, wantIndex, wantCmd)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for index %d", wantIndex)
		}
	}

	recv(1, "a")
	recv(2, "b")

	// entry 3 is not committed yet, so nothing more may arrive
	select {
	case msg := <-r.ApplyCh():
		t.Fatalf("delivered an uncommitted entry: %+v", msg)
	case <-time.After(100 * time.Millisecond):
	}

	r.mu.Lock()
	r.commitIndex = 3
	r.mu.Unlock()
	recv(3, "c")
}

func TestApplierDoesNotHoldLockWhileSending(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.log.Append(LogEntry{Term: 1, Command: "a"})
	r.commitIndex = 1
	r.mu.Unlock()

	go r.applier()
	time.Sleep(50 * time.Millisecond) // applier collects entry 1 and blocks on the send; nobody reads

	done := make(chan struct{})
	go func() {
		r.GetState()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("GetState blocked: the applier holds r.mu while sending")
	}

	<-r.ApplyCh() // drain so the applier can move on
	r.Kill()
}

func TestAgree3(t *testing.T) {
	c := makeCluster(t, 3)
	c.start()
	c.checkOneLeader(t)

	for i, cmd := range []string{"a", "b", "c"} {
		if idx := c.one(t, cmd, 3); idx != i+1 {
			t.Fatalf("command %q landed at index %d, expected %d", cmd, idx, i+1)
		}
	}
}

func TestNoCommitWithoutMajority(t *testing.T) {
	c := makeCluster(t, 3)
	c.start()
	leader := c.checkOneLeader(t)

	c.disconnect((leader + 1) % 3)
	c.disconnect((leader + 2) % 3)

	index, _, ok := c.nodes[leader].Submit("x")
	if !ok {
		t.Fatal("expected the node to still be leader")
	}
	time.Sleep(500 * time.Millisecond)

	if n, _ := c.nCommitted(index); n != 0 {
		t.Fatalf("%d nodes applied index %d without a majority", n, index)
	}
}

func TestFollowerCatchUp(t *testing.T) {
	c := makeCluster(t, 3)
	c.start()
	leader := c.checkOneLeader(t)

	c.one(t, "a", 3)

	follower := (leader + 1) % 3
	c.disconnect(follower)
	c.one(t, "b", 2)
	c.one(t, "c", 2)

	c.reconnect(follower)
	c.one(t, "d", 3)

	// the applier delivers in order, so seeing index 4 means 2 and 3 arrived first
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	for idx, want := range map[int]string{2: "b", 3: "c"} {
		if got := c.applied[follower][idx]; got != want {
			t.Fatalf("node %d applied %v at index %d, expected %q", follower, got, idx, want)
		}
	}
}

func TestLeaderChangeWithUncommitted(t *testing.T) {
	c := makeCluster(t, 3)
	c.start()

	c.one(t, "a", 3)
	old := c.checkOneLeader(t)

	c.disconnect(old)
	c.nodes[old].Submit("x") // never leaves the old leader
	c.nodes[old].Submit("y")

	c.one(t, "b", 2) // the two connected nodes elect a new leader and commit b

	c.reconnect(old)
	c.one(t, "c", 3)

	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	for node := range c.applied {
		for idx, cmd := range c.applied[node] {
			if cmd == "x" || cmd == "y" {
				t.Fatalf("node %d applied uncommitted command %v at index %d", node, cmd, idx)
			}
		}
	}
	if got := c.applied[old][2]; got != "b" {
		t.Fatalf("old leader applied %v at index 2, expected \"b\"", got)
	}
}

func TestChurn5(t *testing.T) {
	runChurn(t, 1, 0)
}

func TestChurn5Unreliable(t *testing.T) {
	runChurn(t, 2, 0.1)
}

// runChurn repeatedly partitions up to 2 of 5 nodes while submitting commands to
// every node, then heals and checks that the cluster converges with no divergence.
// The network flips and the submissions all run on the test goroutine, so
// c.connected needs no lock.
func runChurn(t *testing.T, seed int64, dropRate float64) {
	t.Logf("seed %d, drop rate %v", seed, dropRate)
	rng := rand.New(rand.NewSource(seed))
	c := makeCluster(t, 5)
	if dropRate > 0 {
		c.net.SetDropRate(dropRate)
		c.net.SetDelay(time.Millisecond, 20*time.Millisecond)
	}
	c.start()
	c.one(t, "start", 5)

	nextCmd := 0
	nextFlip := time.Now()
	end := time.Now().Add(4 * time.Second)
	for time.Now().Before(end) {
		if !time.Now().Before(nextFlip) {
			c.net.Heal()
			for i := range c.connected {
				c.connected[i] = true
			}
			// isolate 0, 1 or 2 nodes, so a majority of 3 can always exist
			for _, i := range rng.Perm(len(c.nodes))[:rng.Intn(3)] {
				c.net.Isolate(i)
				c.connected[i] = false
			}
			nextFlip = time.Now().Add(time.Duration(200+rng.Intn(200)) * time.Millisecond)
		}

		// Submit to every node, isolated ones included. Only leaders accept, and a
		// stale isolated leader builds up uncommitted entries that must later be overwritten.
		for _, n := range c.nodes {
			n.Submit(fmt.Sprintf("cmd-%d", nextCmd))
			nextCmd++
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Heal everything, make the network reliable, and require full convergence.
	c.net.Heal()
	c.net.SetDropRate(0)
	c.net.SetDelay(0, 0)
	for i := range c.connected {
		c.connected[i] = true
	}
	last := c.one(t, "final", 5)

	// Everything up to `last` must be identical on every node. The applier delivers
	// in order, so all five nodes have applied 1..last with no gaps.
	c.applyMu.Lock()
	defer c.applyMu.Unlock()
	for idx := 1; idx <= last; idx++ {
		want, ok := c.applied[0][idx]
		if !ok {
			t.Fatalf("node 0 never applied index %d (gap before %d)", idx, last)
		}
		for node := 1; node < len(c.applied); node++ {
			got, ok := c.applied[node][idx]
			if !ok || got != want {
				t.Fatalf("index %d: node 0 has %v, node %d has %v (present=%v)", idx, want, node, got, ok)
			}
		}
	}
	t.Logf("converged: %d entries applied, %d commands submitted", last, nextCmd)
}

func TestApplierExitsWhenKilledWhileBlockedOnSend(t *testing.T) {
	r := newVoter()
	r.mu.Lock()
	r.log.Append(LogEntry{Term: 1, Command: "a"})
	r.commitIndex = 1
	r.mu.Unlock()

	exited := make(chan struct{})
	go func() {
		r.applier()
		close(exited)
	}()
	time.Sleep(50 * time.Millisecond) // applier is now blocked sending; nobody reads

	r.Kill()

	select {
	case <-exited:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("applier is still blocked on the send after Kill")
	}
}
