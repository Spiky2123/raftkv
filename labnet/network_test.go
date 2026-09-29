package labnet

import (
	"encoding/gob"
	"testing"
	"time"
)

import "raftkv/raft"

var _ raft.Transport = (*Endpoint)(nil)

type Msg struct {
	Vals []int
}

func init() { gob.Register(Msg{}) }

func echo(method string, args any) any {
	return "echo: " + args.(string)
}

func TestEcho(t *testing.T) {
	net := NewNetwork(1)
	net.Register(1, echo)

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
	net := NewNetwork(1)
	net.Register(1, echo)

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
	net := NewNetwork(1)
	var reply string

	if net.Call(0, 1, "Echo", "run", &reply) {
		t.Fatal("call to unregistered node should fail")
	}
}

func TestCallDoesNotShareMemory(t *testing.T) {
	net := NewNetwork(1)

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

func TestPartition(t *testing.T) {
	net := NewNetwork(1)
	for id := 1; id <= 3; id++ {
		net.Register(id, echo)
	}
	call := func(from, to int) bool {
		var r string
		return net.Call(from, to, "Echo", "hi", &r)
	}

	net.Partition([]int{1, 2}, []int{3})
	if !call(1, 2) {
		t.Fatal("same side should connect")
	}
	if call(1, 3) || call(3, 1) {
		t.Fatal("cross-partition calls should fail")
	}

	net.Heal()
	if !call(1, 3) {
		t.Fatal("Heal should restore connectivity")
	}

	net.Isolate(1)
	if call(2, 1) || call(1, 2) {
		t.Fatal("isolated node should reach nobody")
	}
	if !call(2, 3) {
		t.Fatal("others should still connect")
	}
}

func TestDropAll(t *testing.T) {
	net := NewNetwork(1)
	net.Register(1, echo)
	net.SetDropRate(1.0)
	var r string
	if net.Call(0, 1, "Echo", "hi", &r) {
		t.Fatal("expected call to be dropped")
	}

	net.SetDropRate(0)
	if !net.Call(0, 1, "Echo", "bye", &r) {
		t.Fatal("expected call to succeed")
	}
}

func TestDelay(t *testing.T) {
	net := NewNetwork(1)
	net.Register(1, echo)
	net.SetDelay(20*time.Millisecond, 20*time.Millisecond)

	start := time.Now()
	var r string
	net.Call(0, 1, "Echo", "hi", &r)
	elapsed := time.Since(start)

	if elapsed < 40*time.Millisecond { // request delay + reply delay
		t.Fatalf("elapsed %v, want at least 40ms", elapsed)
	}
}

func TestSameSeedSameDrops(t *testing.T) {
	run := func(seed int64) []bool {
		net := NewNetwork(seed)
		net.Register(1, echo)
		net.SetDropRate(0.5)
		var results []bool
		var r string
		for i := 0; i <= 50; i++ {
			results = append(results, net.Call(0, 1, "Echo", "hi", &r))
		}
		return results
	}

	a := run(42)
	b := run(42)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("call %d differs: %v vs %v", i, a[i], b[i])
		}
	}

	allSame := true
	for _, v := range a {
		if v != a[0] {
			allSame = false
		}
	}

	if allSame {
		t.Fatal("results are all identical, test isn't random")
	}
}
