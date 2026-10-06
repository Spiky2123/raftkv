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

func TestLogAppend(t *testing.T) {
	l := newLog()
	entry1 := LogEntry{Term: 1}
	entry2 := LogEntry{Term: 1}
	entry3 := LogEntry{Term: 2}

	l.Append(entry1)
	l.Append(entry2)
	l.Append(entry3)

	if l.LastIndex() != 3 {
		t.Fatalf("last index is %v instead of 2", l.LastIndex())
	}

	if l.TermAt(1) != 1 {
		t.Fatal("expected term at index 1 to be 1")
	}

	if l.TermAt(2) != 1 {
		t.Fatal("expected term at index 2 to be 1")
	}

	if l.TermAt(3) != 2 {
		t.Fatal("expected term at index 3 to be 2")
	}
}

func TestLogTermAtMissing(t *testing.T) {
	l := newLog()
	entry1 := LogEntry{Term: 1}
	entry2 := LogEntry{Term: 1}

	l.Append(entry1)
	l.Append(entry2)

	if l.TermAt(-3) != -1 {
		t.Fatalf("Expected log at invalid index to return -1, returned %v instead", l.TermAt(-3))
	}

	if l.TermAt(3) != -1 {
		t.Fatalf("Expected log at invalid index to return -1, returned %v instead", l.TermAt(3))
	}
}
