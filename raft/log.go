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
	if len(l.entries) <= index || index < 0 {
		return -1
	}
	return l.entries[index].Term
}

func (l *Log) Append(entry LogEntry) int {
	l.entries = append(l.entries, entry)
	return len(l.entries) - 1
}

func newLog() *Log {
	return &Log{entries: make([]LogEntry, 1)}
}
