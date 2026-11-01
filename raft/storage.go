package raft

type Storage interface {
	Save(data []byte)
	Load() []byte
}
