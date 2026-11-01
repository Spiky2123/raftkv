package storage

import "sync"

type Memory struct {
	mu   sync.Mutex
	data []byte
}

func NewMemory() *Memory {
	mem := Memory{data: make([]byte, 0)}
	return &mem
}

func (m *Memory) Save(data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make([]byte, len(data))
	copy(m.data, data)
}

func (m *Memory) Load() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(m.data))
	copy(cp, m.data)
	return cp
}
