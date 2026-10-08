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
		t.Fatalf("last index is %v instead of 3", l.LastIndex())
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

func TestLogTruncateFrom(t *testing.T) {
	l := newLog()
	entry1 := LogEntry{Term: 1}
	entry2 := LogEntry{Term: 1}
	entry3 := LogEntry{Term: 2}
	entry4 := LogEntry{Term: 2}

	l.Append(entry1)
	l.Append(entry2)
	l.Append(entry3)
	l.Append(entry4)

	l.TruncateFrom(3)

	if l.LastIndex() != 2 {
		t.Fatalf("expected last index to be 2, was %v instead", l.LastIndex())
	}

	if l.TermAt(1) != 1 {
		t.Fatalf("expected term at index 1 to be 1, was %v instead", l.TermAt(1))
	}

	if l.TermAt(2) != 1 {
		t.Fatalf("expected term at index 2 to be 1, was %v instead", l.TermAt(2))
	}

	if l.TermAt(3) != -1 {
		t.Fatalf("expected term at call for index 3 to fail, got %v instead", l.TermAt(3))
	}

	l.Append(LogEntry{Term: 3})

	if l.LastIndex() != 3 {
		t.Fatalf("expected last index to be 3, was %v instead", l.LastIndex())
	}

	if l.TermAt(3) != 3 {
		t.Fatalf("expected term at call for index 3 to be 3, got %v instead", l.TermAt(3))
	}
}

func TestLogTruncateAtMissing(t *testing.T) {
	l := newLog()
	entry1 := LogEntry{Term: 1}
	entry2 := LogEntry{Term: 1}

	l.Append(entry1)
	l.Append(entry2)

	l.TruncateFrom(0)

	if l.LastIndex() != 2 {
		t.Fatalf("expected last index to be 2, was %v instead", l.LastIndex())
	}

	l.TruncateFrom(l.LastIndex() + 2)

	if l.LastIndex() != 2 {
		t.Fatalf("expected last index to be 2, was %v instead", l.LastIndex())
	}
}

func TestLogSliceIsCopy(t *testing.T) {
	l := newLog()
	entry1 := LogEntry{Term: 1}
	entry2 := LogEntry{Term: 1}
	entry3 := LogEntry{Term: 2}

	l.Append(entry1)
	l.Append(entry2)
	l.Append(entry3)

	s := l.Slice(2)

	if len(s) != 2 {
		t.Fatalf("expected slice to have length 2, has length %v instead", len(s))
	}

	if s[1].Term != 2 {
		t.Fatalf("expected second log entry to have term 2, has term %v instead", s[1].Term)
	}

	l.TruncateFrom(2)
	l.Append(LogEntry{Term: 5})

	if s[0].Term != 1 {
		t.Fatalf("first log entry term changed from 1 to %v", s[0].Term)
	}
}

func TestLogSliceEmpty(t *testing.T) {
	l := newLog()
	entry1 := LogEntry{Term: 1}
	entry2 := LogEntry{Term: 1}
	entry3 := LogEntry{Term: 2}

	l.Append(entry1)
	l.Append(entry2)
	l.Append(entry3)

	s := l.Slice(len(l.entries) + 1)

	if len(s) != 0 {
		t.Fatal("expected invalid slice to have length 0")
	}
}
