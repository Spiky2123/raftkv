package labnet

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)

type Network struct {
	mu                 sync.Mutex
	handlers           map[int]Handler // node id -> function
	group              map[int]int
	nextGroup          int
	rng                *rand.Rand
	dropRate           float64
	minDelay, maxDelay time.Duration
}

type Handler func(method string, args any) any

func NewNetwork(seed int64) *Network {
	return &Network{
		handlers: make(map[int]Handler),
		group:    make(map[int]int),
		rng:      rand.New(rand.NewSource(seed)),
	}
}

func (n *Network) Register(id int, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

func (n *Network) Call(from, to int, method string, args, reply any) bool {
	n.mu.Lock()
	h := n.handlers[to]
	ok := n.Reachable(from, to)
	dropReq := n.rng.Float64() < n.dropRate
	dropReply := n.rng.Float64() < n.dropRate
	reqDelay := n.randDelay()
	replyDelay := n.randDelay()
	n.mu.Unlock()

	time.Sleep(reqDelay)
	if h == nil || !ok || dropReq {
		return false
	}

	n.mu.Lock()
	ok = n.Reachable(from, to) // Check in case reachable status changes while sleeping for the delay
	n.mu.Unlock()
	if !ok {
		return false
	}

	result := h(method, deepCopy(args))

	if dropReply {
		return false
	}

	time.Sleep(replyDelay)
	copyInto(reply, result)
	return true
}

func (n *Network) Reachable(from, to int) bool { // caller must hold n.mu
	return n.group[from] == n.group[to]
}

func (n *Network) Partition(a, b []int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextGroup++
	for _, id := range a {
		n.group[id] = n.nextGroup
	}
	n.nextGroup++
	for _, id := range b {
		n.group[id] = n.nextGroup
	}
}

func (n *Network) Isolate(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nextGroup++
	n.group[id] = n.nextGroup
}

func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.group = make(map[int]int)
}

func (n *Network) SetDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

func (n *Network) SetDelay(min, max time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.minDelay, n.maxDelay = min, max
}

func (n *Network) randDelay() time.Duration {
	if n.maxDelay <= n.minDelay {
		return n.minDelay
	}
	return n.minDelay + time.Duration(n.rng.Int63n(int64(n.maxDelay-n.minDelay)))
}

type Endpoint struct {
	net *Network
	id  int
}

func (n *Network) Endpoint(id int) *Endpoint {
	return &Endpoint{net: n, id: id}
}

func (e *Endpoint) Call(to int, method string, args, reply any) bool {
	return e.net.Call(e.id, to, method, args, reply)
}

func deepCopy(v any) any {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(&v); err != nil {
		panic(err)
	}
	var out any
	if err := gob.NewDecoder(&buf).Decode(&out); err != nil {
		panic(err)
	}
	return out
}

func copyInto(dst, src any) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		panic(err)
	}
	if err := gob.NewDecoder(&buf).Decode(dst); err != nil {
		panic(err)
	}
}
