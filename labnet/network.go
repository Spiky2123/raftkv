package labnet

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type Network struct {
	mu       sync.Mutex
	handlers map[int]Handler // node id -> function
}

type Handler func(method string, args any) any

func NewNetwork() *Network {
	return &Network{handlers: make(map[int]Handler)}
}

func (n *Network) Register(id int, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

func (n *Network) Call(from, to int, method string, args, reply any) bool {
	n.mu.Lock()
	h := n.handlers[to]
	n.mu.Unlock()

	if h == nil {
		return false
	}

	result := h(method, deepCopy(args))
	copyInto(reply, result)

	return true
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
