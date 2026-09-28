package raft

type Transport interface {
	Call(to int, method string, args, reply any) bool
}
