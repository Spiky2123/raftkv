package raft

import (
	"math/rand"
	"sync"
)

const noVote = -1

type State int

const (
	follower State = iota
	candidate
	leader
)

type Raft struct {
	mu          sync.Mutex
	id          int
	state       State
	currentTerm int
	votedFor    int
	peers       []int
	transport   Transport
	rng         *rand.Rand
}

func New(id int, peers []int, transport Transport, rng *rand.Rand) *Raft {
	raft := Raft{
		id:          id,
		state:       follower,
		currentTerm: 0,
		votedFor:    noVote,
		peers:       peers,
		transport:   transport,
		rng:         rng,
	}
	return &raft
}

func (r *Raft) GetState() (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.state == leader
}
