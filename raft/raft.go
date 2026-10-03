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
			r.becomeFollowerLocked(request.Term)
		}

		if r.votedFor == noVote || r.votedFor == request.CandidateID {
			r.votedFor = request.CandidateID
			r.resetElectionTimerLocked()

			reply.VoteGranted = true
		}
		reply.Term = r.currentTerm
		return reply
	case "AppendEntries":
		r.mu.Lock()
		defer r.mu.Unlock()
		appendArgs := args.(AppendEntriesArgs)
		reply := AppendEntriesReply{Term: r.currentTerm}
		if appendArgs.Term < r.currentTerm {
			return reply
		} else if appendArgs.Term > r.currentTerm {
			r.becomeFollowerLocked(appendArgs.Term)
			reply.Term = r.currentTerm
		}

		if appendArgs.Term == r.currentTerm && r.state == candidate {
			r.state = follower
		}
		r.resetElectionTimerLocked()
		return reply
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

// caller must hold r.mu
func (r *Raft) resetElectionTimerLocked() {
	timeout := 150*time.Millisecond +
		time.Duration(r.rng.Int63n(150))*time.Millisecond
	r.electionDeadline = time.Now().Add(timeout)
}

// caller must hold r.mu
func (r *Raft) becomeFollowerLocked(term int) {
	r.currentTerm = term
	r.votedFor = noVote
	r.state = follower
	r.resetElectionTimerLocked()
}

func (r *Raft) leaderLoop(term int) {
	for {
		r.mu.Lock()
		if r.state != leader || r.currentTerm != term {
			r.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{Term: term, LeaderID: r.id}
		for _, v := range r.peers {
			if v == r.id {
				continue
			}
			go func(peer int) {
				var reply AppendEntriesReply
				if r.transport.Call(peer, "AppendEntries", args, &reply) {
					r.mu.Lock()
					if reply.Term > r.currentTerm {
						r.becomeFollowerLocked(reply.Term)
					}
					r.mu.Unlock()
				}
			}(v)
		}
		r.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

}

// caller must hold r.mu
func (r *Raft) startElectionLocked() {
	r.resetElectionTimerLocked()
	r.currentTerm++
	r.state = candidate
	r.votedFor = r.id
	req := RequestVoteArgs{Term: r.currentTerm, CandidateID: r.id}
	electionTerm := r.currentTerm
	votes := 1
	for _, v := range r.peers {
		if v == r.id {
			continue
		}
		go func(peer int) {
			var reply RequestVoteReply
			if r.transport.Call(peer, "RequestVote", req, &reply) {
				r.mu.Lock()
				defer r.mu.Unlock()
				if reply.Term > r.currentTerm {
					r.becomeFollowerLocked(reply.Term)
					return
				}
				if r.currentTerm != electionTerm || r.state != candidate {
					return
				}
				if reply.VoteGranted {
					votes++
					if votes >= len(r.peers)/2+1 {
						r.state = leader
						go r.leaderLoop(electionTerm)
					}
					return
				}
			}
		}(v)
	}
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
