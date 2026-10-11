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
	dead             bool
	currentTerm      int
	log              *Log
	votedFor         int
	electionDeadline time.Time
	peers            []int
	nextIndex        []int
	matchIndex       []int
	commitIndex      int
	lastApplied      int
	applyCh          chan ApplyMsg
	transport        Transport
	rng              *rand.Rand
}

func New(id int, peers []int, transport Transport, rng *rand.Rand) *Raft {
	raft := Raft{
		id:          id,
		state:       follower,
		dead:        false,
		currentTerm: 0,
		log:         newLog(),
		votedFor:    noVote,
		peers:       peers,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     make(chan ApplyMsg),
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
		if r.dead {
			return reply
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
		reply := AppendEntriesReply{Term: r.currentTerm, Success: false}
		if r.dead {
			return reply
		}
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

		if r.log.TermAt(appendArgs.PrevLogIndex) == appendArgs.PrevLogTerm {
			reply.Success = true
			if r.commitIndex < min(appendArgs.LeaderCommit, appendArgs.PrevLogIndex+len(appendArgs.Entries)) {
				r.commitIndex = min(appendArgs.LeaderCommit, appendArgs.PrevLogIndex+len(appendArgs.Entries))
			}

			for k, entry := range appendArgs.Entries {
				index := appendArgs.PrevLogIndex + 1 + k
				if r.log.TermAt(index) == -1 {
					r.log.Append(entry)
				} else {
					if r.log.TermAt(index) != entry.Term {
						r.log.TruncateFrom(index)
						r.log.Append(entry)
					} else {
						continue
					}
				}
			}
		}

		return reply
	}
	return nil
}

func (r *Raft) Submit(cmd any) (index, term int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state == leader {
		r.log.Append(LogEntry{Term: r.currentTerm, Command: cmd})
	}
	return r.log.LastIndex(), r.currentTerm, r.state == leader
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
	go r.applier()
}

func (r *Raft) Kill() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.dead = true
}

func (r *Raft) ApplyCh() <-chan ApplyMsg {
	return r.applyCh
}

func (r *Raft) applier() {
	for {
		r.mu.Lock()
		if r.dead {
			r.mu.Unlock()
			return
		}
		msgs := r.collectApplyLocked()
		r.mu.Unlock()
		for _, m := range msgs {
			r.applyCh <- m
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// caller must hold r.mu
func (r *Raft) collectApplyLocked() []ApplyMsg {
	n := r.commitIndex - r.lastApplied
	if n <= 0 {
		return []ApplyMsg{}
	}
	slice := r.log.Slice(r.lastApplied + 1)
	result := make([]ApplyMsg, n)
	for i := range result {
		result[i].Command = slice[i].Command
		result[i].Index = r.lastApplied + 1 + i
	}
	if r.lastApplied < r.commitIndex {
		r.lastApplied = r.commitIndex
	}
	return result
}

// caller must hold r.mu
func (r *Raft) advanceCommitIndexLocked() {
	var cnt int
	for i := r.log.LastIndex(); i > r.commitIndex; i-- {
		if r.log.TermAt(i) < r.currentTerm {
			return
		}
		cnt = 1
		for _, j := range r.peers {
			if r.matchIndex[j] >= i {
				cnt++
			}
		}
		if cnt >= len(r.peers)/2+1 {
			r.commitIndex = i
			return
		}
	}
}

// caller must hold r.mu
func (r *Raft) buildAppendEntriesLocked(peer int) AppendEntriesArgs {
	index := r.nextIndex[peer] - 1
	return AppendEntriesArgs{
		Term:         r.currentTerm,
		LeaderID:     r.id,
		LeaderCommit: r.commitIndex,
		PrevLogIndex: index,
		PrevLogTerm:  r.log.TermAt(index),
		Entries:      r.log.Slice(index + 1),
	}
}

// caller must hold r.mu
func (r *Raft) handleAppendEntriesReplyLocked(peer int, args AppendEntriesArgs, reply AppendEntriesReply) {
	if reply.Term > r.currentTerm {
		r.becomeFollowerLocked(reply.Term)
		return
	}
	if r.state != leader || r.currentTerm != args.Term {
		return
	}
	if reply.Success {
		if r.matchIndex[peer] < args.PrevLogIndex+len(args.Entries) {
			r.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
			r.nextIndex[peer] = args.PrevLogIndex + len(args.Entries) + 1
			r.advanceCommitIndexLocked()
		}
	} else {
		if r.nextIndex[peer] > args.PrevLogIndex {
			r.nextIndex[peer] = max(1, args.PrevLogIndex)
		}
	}
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
		if r.dead {
			r.mu.Unlock()
			return
		}
		if r.state != leader || r.currentTerm != term {
			r.mu.Unlock()
			return
		}

		for _, v := range r.peers {
			if v == r.id {
				continue
			}
			args := r.buildAppendEntriesLocked(v)
			go func(peer int, args AppendEntriesArgs) {
				var reply AppendEntriesReply
				if r.transport.Call(peer, "AppendEntries", args, &reply) {
					r.mu.Lock()
					r.handleAppendEntriesReplyLocked(peer, args, reply)
					r.mu.Unlock()
				}
			}(v, args)
		}
		r.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

}

// caller must hold r.mu
func (r *Raft) becomeLeaderLocked() {
	r.state = leader
	r.nextIndex = make([]int, len(r.peers))
	for i := range r.nextIndex {
		r.nextIndex[i] = r.log.LastIndex() + 1
	}
	r.matchIndex = make([]int, len(r.peers))
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
						r.becomeLeaderLocked()
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
		if r.dead {
			r.mu.Unlock()
			return
		}
		if time.Now().After(r.electionDeadline) && r.state != leader {
			r.startElectionLocked()
		}
		r.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
