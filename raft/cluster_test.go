package raft

import (
	"math/rand"
	"raftkv/labnet"
	"testing"
	"time"
)

type cluster struct {
	net   *labnet.Network
	nodes []*Raft
}

func makeCluster(t *testing.T, n int) *cluster {
	net := labnet.NewNetwork(1)
	nodes := make([]*Raft, n)
	peers := make([]int, n)
	for i := 0; i < n; i++ {
		peers[i] = i
	}
	for i := 0; i < n; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		end := net.Endpoint(i)
		nodes[i] = New(i, peers, end, rng)
		net.Register(i, nodes[i].Handle)
	}
	c := cluster{
		net:   net,
		nodes: nodes,
	}
	return &c
}

func (c *cluster) checkOneLeader(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		leaderID := -1
		for _, v := range c.nodes {
			_, isLeader := v.GetState()
			if isLeader {
				if leaderID == -1 {
					leaderID = v.id
				} else {
					t.Fatal("multiple leaders elected")
				}
			}
		}
		if leaderID != -1 {
			return leaderID
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatal("no leader was elected")
	return -1
}

func TestElection3(t *testing.T) {
	t.Skip("election not implemented yet")
	c := makeCluster(t, 3)
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
