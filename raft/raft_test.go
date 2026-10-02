package raft

import (
	"math/rand"
	"testing"
)

func TestInitialState(t *testing.T) {
	peers := []int{0, 1, 2}
	rng := rand.New(rand.NewSource(1))
	node := New(0, peers, nil, rng)

	term, isLeader := node.GetState()

	if term != 0 {
		t.Fatalf("node term is %d instead of 0", term)
	}

	if isLeader {
		t.Fatal("node expected to not be leader")
	}
}
