package labnet

import (
	"encoding/gob"
	"testing"
)

import "raftkv/raft"

var _ raft.Transport = (*Endpoint)(nil)

type Msg struct {
	Vals []int
}

func init() { gob.Register(Msg{}) }

func TestEcho(t *testing.T) {
	net := NewNetwork()
	net.Register(1, func(method string, args any) any {
		return "echo: " + args.(string)
	})

	var reply string
	ok := net.Call(0, 1, "Echo", "hello", &reply)

	if !ok {
		t.Fatal("call failed")
	}
	if reply != "echo: hello" {
		t.Fatalf("got %q, want %q", reply, "echo: hello")
	}
}

func TestEndpoint(t *testing.T) {
	net := NewNetwork()
	net.Register(1, func(method string, args any) any {
		return "echo: " + args.(string)
	})

	ep := net.Endpoint(0)

	var reply string
	if !ep.Call(1, "Echo", "hi", &reply) {
		t.Fatal("call failed")
	}
	if reply != "echo: hi" {
		t.Fatalf("got %q", reply)
	}
}

func TestUnregisteredCall(t *testing.T) {
	net := NewNetwork()
	var reply string

	if net.Call(0, 1, "Echo", "run", &reply) {
		t.Fatal("call to unregistered node should fail")
	}
}

func TestCallDoesNotShareMemory(t *testing.T) {
	net := NewNetwork()

	net.Register(1, func(method string, args any) any {
		m := args.(Msg)
		m.Vals[0] = 99
		return "done"
	})

	original := Msg{Vals: []int{1, 2, 3}}
	var reply string

	net.Call(0, 1, "Mutate", original, &reply)

	if original.Vals[0] != 1 {
		t.Fatalf("caller's slice was modified: Vals[0] = %d, want 1", original.Vals[0])
	}
}
