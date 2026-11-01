package storage

import (
	"bytes"
	"testing"
)

func TestMemoryRoundTrip(t *testing.T) {
	s := NewMemory()

	if len(s.Load()) != 0 {
		t.Fatal("expected new memory to be initialized with length 0")
	}

	value := []byte{21, 213, 12, 3, 4}
	s.Save(value)
	got := s.Load()
	if !bytes.Equal(got, value) {
		t.Fatalf("expected %s, got %s instead", value, got)
	}
	value = []byte{6, 7, 8, 9, 6, 7, 10}
	s.Save(value)
	got = s.Load()
	if !bytes.Equal(got, value) {
		t.Fatalf("expected %s, got %s instead", value, got)
	}
}
