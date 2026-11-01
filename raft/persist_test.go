package raft

import (
	"math/rand"
	"raftkv/storage"
	"testing"
)

func TestPersistAndRestore(t *testing.T) {
	mem := storage.NewMemory()
	peers := []int{0, 1, 2}

	first := New(0, peers, nil, mem, rand.New(rand.NewSource(1)))
	first.mu.Lock()
	first.currentTerm = 7
	first.votedFor = 2
	first.log.Append(LogEntry{Term: 1, Command: 10})
	first.log.Append(LogEntry{Term: 4, Command: 20})
	first.persist()
	first.mu.Unlock()

	second := New(0, peers, nil, mem, rand.New(rand.NewSource(1)))

	second.mu.Lock()
	defer second.mu.Unlock()
	if second.currentTerm != 7 {
		t.Fatalf("currentTerm = %d, want 7", second.currentTerm)
	}
	if second.votedFor != 2 {
		t.Fatalf("votedFor = %d, want 2", second.votedFor)
	}
	if second.log.LastIndex() != 2 {
		t.Fatalf("log.LastIndex() = %d, want 2", second.log.LastIndex())
	}
	if got := second.log.TermAt(1); got != 1 {
		t.Fatalf("log term at index 1 = %d, want 1", got)
	}
	if got := second.log.TermAt(2); got != 4 {
		t.Fatalf("log term at index 2 = %d, want 4", got)
	}
	if got := second.log.entries[1].Command; got != 10 {
		t.Fatalf("command at index 1 = %v, want 10", got)
	}
	if got := second.log.entries[2].Command; got != 20 {
		t.Fatalf("command at index 2 = %v, want 20", got)
	}
}
