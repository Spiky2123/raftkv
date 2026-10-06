package raft

type LogEntry struct {
	Term    int
	Command any
}

type Log struct {
	entries []LogEntry
}

func (l *Log) LastIndex() int {
	return len(l.entries) - 1
}

func (l *Log) TermAt(index int) int {
	return l.entries[index].Term
}

func newLog() *Log {
	return &Log{entries: make([]LogEntry, 1)}
}
