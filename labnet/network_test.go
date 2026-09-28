package labnet

import "testing"
import "raftkv/raft"

var _ raft.Transport = (*Endpoint)(nil)

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
