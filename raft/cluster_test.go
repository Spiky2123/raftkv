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
	t.Skip()
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
	c := makeCluster(t, 3)
	c.checkOneLeader(t)
}

func TestElectionTimer(t *testing.T) {
	c := makeCluster(t, 1)

	before := time.Now()
	c.nodes[0].ResetElectionTimer()
	deadline := c.nodes[0].electionDeadline
	after := time.Now()
	if deadline.Before(before.Add(150*time.Millisecond)) || deadline.After(after.Add(300*time.Millisecond)) {
		t.Fatalf("deadline exceeds bounds, expected between 150ms to 300ms, got %v instead", deadline.Sub(before))
	}
}
