package raft

import (
	"math/rand"
	"sync"
	"time"
)

const noVote = -1

type State int

const (
	follower State = iota
	candidate
	leader
)

type Raft struct {
	mu               sync.Mutex
	id               int
	state            State
	currentTerm      int
	votedFor         int
	electionDeadline time.Time
	peers            []int
	transport        Transport
	rng              *rand.Rand
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

func (r *Raft) Handle(method string, args any) any {
	switch method {
	case "RequestVote":
		r.mu.Lock()
		defer r.mu.Unlock()
		request := args.(RequestVoteArgs)
		reply := RequestVoteReply{
			Term:        r.currentTerm,
			VoteGranted: false,
		}
		if request.Term < r.currentTerm {
			return reply
		} else if request.Term > r.currentTerm {
			r.votedFor = noVote
			r.currentTerm = request.Term
			r.state = follower
		}

		if r.votedFor == noVote || r.votedFor == request.CandidateID {
			r.votedFor = request.CandidateID
			r.resetElectionTimerLocked()

			reply.VoteGranted = true
		}
		reply.Term = r.currentTerm
		return reply
	case "AppendEntries":
		return nil
	}
	return nil
}

func (r *Raft) GetState() (int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.currentTerm, r.state == leader
}

func (r *Raft) Start() {
	r.mu.Lock()
	r.resetElectionTimerLocked()
	r.mu.Unlock()
	go r.ticker()
}

func (r *Raft) resetElectionTimerLocked() { // caller must hold r.mu
	timeout := 150*time.Millisecond +
		time.Duration(r.rng.Int63n(150))*time.Millisecond
	r.electionDeadline = time.Now().Add(timeout)
}

func (r *Raft) startElectionLocked() { // caller must hold r.mu
	r.resetElectionTimerLocked()
	r.currentTerm++
	r.state = candidate
	r.votedFor = r.id
}

func (r *Raft) ticker() {
	for {
		r.mu.Lock()
		if time.Now().After(r.electionDeadline) && r.state != leader {
			r.startElectionLocked()
		}
		r.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
