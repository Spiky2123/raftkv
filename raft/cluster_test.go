package raft

import (
	"math/rand"
	"raftkv/labnet"
	"runtime"
	"testing"
	"time"
)

type cluster struct {
	net       *labnet.Network
	nodes     []*Raft
	connected []bool
}

func makeCluster(t *testing.T, n int) *cluster {
	net := labnet.NewNetwork(1)
	nodes := make([]*Raft, n)
	peers := make([]int, n)
	connected := make([]bool, n)
	for i := 0; i < n; i++ {
		peers[i] = i
		connected[i] = true
	}
	for i := 0; i < n; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		end := net.Endpoint(i)
		nodes[i] = New(i, peers, end, rng)
		net.Register(i, nodes[i].Handle)
	}
	c := cluster{
		net:       net,
		nodes:     nodes,
		connected: connected,
	}
	return &c
}

func (c *cluster) checkOneLeader(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		leaders := make(map[int][]int) // term -> leader IDs
		for i, n := range c.nodes {
			if !c.connected[i] {
				continue
			}
			if term, isLeader := n.GetState(); isLeader {
				leaders[term] = append(leaders[term], i)
			}
		}
		best := -1
		bestID := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				t.Fatalf("two leaders in term %d: %v", term, ids)
			}
			if term > best {
				best, bestID = term, ids[0]
			}
		}
		if bestID != -1 {
			return bestID
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader was elected")
	return -1
}

func (c *cluster) disconnect(i int) {
	c.net.Isolate(i)
	c.connected[i] = false
}

func (c *cluster) reconnect(i int) {
	c.net.Heal()
	c.connected[i] = true
}

func (c *cluster) kill(i int) {
	c.nodes[i].Kill()
	c.net.Unregister(i)
}

func TestElection3(t *testing.T) {
	c := makeCluster(t, 3)
	c.start()
	c.checkOneLeader(t)
}

func TestElection5(t *testing.T) {
	c := makeCluster(t, 5)
	c.start()
	c.checkOneLeader(t)
}

func TestElectionTimer(t *testing.T) {
	c := makeCluster(t, 1)
	lo := 50 * time.Second
	hi := 0 * time.Second
	for i := 0; i < 100; i++ {
		before := time.Now()

		c.nodes[0].mu.Lock()
		c.nodes[0].resetElectionTimerLocked()
		deadline := c.nodes[0].electionDeadline

		c.nodes[0].mu.Unlock()
		after := time.Now()

		if deadline.Before(before.Add(150*time.Millisecond)) || deadline.After(after.Add(300*time.Millisecond)) {
			t.Fatalf("deadline exceeds bounds, expected between 150ms to 300ms, got %v instead", deadline.Sub(before))
		}

		d := deadline.Sub(before)
		if d < lo {
			lo = d
		}
		if d > hi {
			hi = d
		}
	}

	if hi-lo < 100*time.Millisecond {
		t.Fatalf("spread too small: min %v, max %v", lo, hi)
	}
}

func TestStartElection(t *testing.T) {
	c := makeCluster(t, 3)

	c.net.Isolate(1)
	c.net.Isolate(2)

	c.nodes[0].Start()
	time.Sleep(600 * time.Millisecond)

	term, isLeader := c.nodes[0].GetState()

	t.Logf("term = %d", term)

	if term == 0 {
		t.Fatal("election did not start")
	}
	if isLeader {
		t.Fatal("node won election without majority of the votes")
	}
}

func TestRequestVoteGrantsFreshFollower(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	voter := New(1, []int{0, 1, 2}, nil, rng)

	args := RequestVoteArgs{Term: 1, CandidateID: 0}
	reply := voter.Handle("RequestVote", args).(RequestVoteReply)

	if !reply.VoteGranted {
		t.Fatal("VoteGranted = false, want true")
	}
	if reply.Term != 1 {
		t.Fatalf("reply.Term = %d, want 1", reply.Term)
	}

	voter.mu.Lock()
	defer voter.mu.Unlock()
	if voter.currentTerm != 1 {
		t.Fatalf("currentTerm = %d, want 1", voter.currentTerm)
	}
	if voter.votedFor != 0 {
		t.Fatalf("votedFor = %d, want 0", voter.votedFor)
	}
}

func newVoter() *Raft {
	return New(1, []int{0, 1, 2}, nil, rand.New(rand.NewSource(1)))
}

func requestVote(r *Raft, term, candidate int) RequestVoteReply {
	args := RequestVoteArgs{Term: term, CandidateID: candidate}
	return r.Handle("RequestVote", args).(RequestVoteReply)
}

func peek(r *Raft) (term, votedFor int, st State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.votedFor, r.state
}

func TestVoteHigherTermOverridesOldVote(t *testing.T) {
	v := newVoter()
	requestVote(v, 1, 0)
	reply := requestVote(v, 2, 2)
	term, votedFor, _ := peek(v)
	if !reply.VoteGranted || reply.Term != 2 || term != 2 || votedFor != 2 {
		t.Fatalf("reply=%+v term=%d votedFor=%d, want granted, term 2, votedFor 2", reply, term, votedFor)
	}
}

func TestVoteCandidateStepsDown(t *testing.T) {
	v := newVoter()
	v.mu.Lock()
	v.state, v.currentTerm, v.votedFor = candidate, 3, v.id
	v.mu.Unlock()
	reply := requestVote(v, 4, 0)
	_, _, st := peek(v)
	if !reply.VoteGranted || st != follower {
		t.Fatalf("reply=%+v state=%d, want granted and follower", reply, st)
	}
}

func TestVoteLowerTermRejected(t *testing.T) {
	v := newVoter()
	v.mu.Lock()
	v.currentTerm = 3
	v.mu.Unlock()
	reply := requestVote(v, 2, 0)
	term, votedFor, _ := peek(v)
	if reply.VoteGranted || reply.Term != 3 || term != 3 || votedFor != noVote {
		t.Fatalf("reply=%+v term=%d votedFor=%d, want rejected, term 3, no vote", reply, term, votedFor)
	}
}

func TestVoteSameTermOtherCandidateRejected(t *testing.T) {
	v := newVoter()
	requestVote(v, 1, 0)
	reply := requestVote(v, 1, 2)
	_, votedFor, _ := peek(v)
	if reply.VoteGranted || votedFor != 0 {
		t.Fatalf("reply=%+v votedFor=%d, want rejected and votedFor 0", reply, votedFor)
	}
}

func TestVoteSameCandidateIdempotent(t *testing.T) {
	v := newVoter()
	if !requestVote(v, 1, 0).VoteGranted || !requestVote(v, 1, 0).VoteGranted {
		t.Fatal("repeat request from the same candidate should be granted")
	}
}

func TestCandidateWins(t *testing.T) {
	c := makeCluster(t, 3)
	c.nodes[0].Start()
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		_, isLeader := c.nodes[0].GetState()

		if isLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("node 0 failed to be elected as leader")
}

func TestCandidateWinsWithOneNodeDown(t *testing.T) {
	c := makeCluster(t, 3)
	c.net.Isolate(2)
	c.nodes[0].Start()
	deadline := time.Now().Add(2 * time.Second)

	for time.Now().Before(deadline) {
		_, isLeader := c.nodes[0].GetState()

		if isLeader {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("node 0 failed to be elected as leader")
}

// start launches every node's ticker.
func (c *cluster) start() {
	for _, n := range c.nodes {
		n.Start()
	}
}

func TestLeaderSendsHeartbeats(t *testing.T) {
	c := makeCluster(t, 3)
	c.start()

	leader := c.checkOneLeader(t)
	term, _ := c.nodes[leader].GetState()

	time.Sleep(1 * time.Second)

	leader2 := c.checkOneLeader(t)
	term2, _ := c.nodes[leader2].GetState()
	if leader2 != leader || term2 != term {
		t.Fatalf("leadership changed: leader %d->%d, term %d->%d (heartbeats are not keeping followers quiet)",
			leader, leader2, term, term2)
	}
}

func TestReElection(t *testing.T) {
	c := makeCluster(t, 3)
	c.start()

	leader1 := c.checkOneLeader(t)
	term1, _ := c.nodes[leader1].GetState()

	c.disconnect(leader1)
	leader2 := c.checkOneLeader(t)
	term2, _ := c.nodes[leader2].GetState()
	if leader2 == leader1 || term2 <= term1 {
		t.Fatalf("expected a new leader in a higher term: leader %d->%d, term %d->%d",
			leader1, leader2, term1, term2)
	}

	c.reconnect(leader1)
	time.Sleep(1 * time.Second)

	leader3 := c.checkOneLeader(t)
	term3, _ := c.nodes[leader3].GetState()
	if leader3 != leader2 || term3 != term2 {
		t.Fatalf("returning node disrupted the cluster: leader %d->%d, term %d->%d",
			leader2, leader3, term2, term3)
	}
	if _, isLeader := c.nodes[leader1].GetState(); isLeader {
		t.Fatal("old leader still thinks it is leader")
	}
}

type fakeTransport struct{ replyTerm int }

func (f fakeTransport) Call(to int, method string, args, reply any) bool {
	if r, ok := reply.(*AppendEntriesReply); ok {
		r.Term = f.replyTerm
	}
	return true
}

func TestLeaderStepDownResetsTimer(t *testing.T) {
	r := New(0, []int{0, 1, 2}, fakeTransport{replyTerm: 9}, rand.New(rand.NewSource(1)))
	r.mu.Lock()
	r.state, r.currentTerm = leader, 1
	r.electionDeadline = time.Now().Add(-time.Hour)
	r.mu.Unlock()

	go r.leaderLoop(1)
	time.Sleep(100 * time.Millisecond)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != follower || r.currentTerm != 9 {
		t.Fatalf("state=%d term=%d, want follower in term 9", r.state, r.currentTerm)
	}
	if !r.electionDeadline.After(time.Now()) {
		t.Fatal("stepping down left a stale election deadline; the ticker will start an election at once")
	}
}

func TestAtMostOneLeaderPerTerm(t *testing.T) {
	c := makeCluster(t, 5)
	c.net.SetDropRate(0.2)
	c.net.SetDelay(5*time.Millisecond, 40*time.Millisecond)
	c.start()

	leaderOfTerm := make(map[int]int) // term -> the node we saw lead it
	isolated := -1
	lastFlip := time.Now()
	end := time.Now().Add(4 * time.Second)

	for time.Now().Before(end) {
		currentLeader := -1
		for i, n := range c.nodes {
			term, isLeader := n.GetState() // term and role read together under one lock
			if !isLeader {
				continue
			}
			if prev, ok := leaderOfTerm[term]; ok && prev != i {
				t.Fatalf("two leaders in term %d: node %d and node %d", term, prev, i)
			}
			leaderOfTerm[term] = i
			if i != isolated {
				currentLeader = i
			}
		}

		// Every ~600ms: isolate the current leader, or heal the network.
		if time.Since(lastFlip) > 600*time.Millisecond {
			if isolated == -1 {
				if currentLeader != -1 {
					c.net.Isolate(currentLeader)
					isolated = currentLeader
					lastFlip = time.Now()
				}
			} else {
				c.net.Heal()
				isolated = -1
				lastFlip = time.Now()
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Guard against a test that passes because nothing happened.
	if len(leaderOfTerm) < 2 {
		t.Fatalf("only %d term(s) had a leader, the cluster didn't churn enough: %v",
			len(leaderOfTerm), leaderOfTerm)
	}
	t.Logf("leaders by term: %v", leaderOfTerm)
}

func TestKillStopsGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	c := makeCluster(t, 3)
	c.start()
	c.checkOneLeader(t) // a leaderLoop is running too

	for _, n := range c.nodes {
		n.Kill()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+2 { // small slack for in-flight RPC goroutines
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutines still running: before=%d now=%d", before, runtime.NumGoroutine())
}

func TestKilledNodeStaysDead(t *testing.T) {
	r := newVoter()
	r.Kill()

	reply := requestVote(r, 5, 0)

	term, votedFor, _ := peek(r)
	if reply.VoteGranted || term == 5 || votedFor != noVote {
		t.Fatalf("killed node reacted: reply=%+v term=%d votedFor=%d", reply, term, votedFor)
	}
}
