package raft

import "testing"

func TestLogEmpty(t *testing.T) {
	l := newLog()

	if l.LastIndex() != 0 {
		t.Fatalf("last index is %v instead of 0", l.LastIndex())
	}

	if l.TermAt(0) != 0 {
		t.Fatal("expected term at index 0 to be 0")
	}
}
