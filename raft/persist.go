package raft

import (
	"bytes"
	"encoding/gob"
)

// caller must hold r.mu
func (r *Raft) persist() error {
	buf := bytes.Buffer{}
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(r.currentTerm)
	if err != nil {
		return err
	}
	err = encoder.Encode(r.votedFor)
	if err != nil {
		return err
	}
	err = r.log.encode(encoder)
	if err != nil {
		return err
	}
	r.storage.Save(buf.Bytes())
	return nil
}

func (r *Raft) readPersist() error {
	data := r.storage.Load()
	if len(data) == 0 {
		return nil
	}
	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(&r.currentTerm)
	if err != nil {
		return err
	}
	err = decoder.Decode(&r.votedFor)
	if err != nil {
		return err
	}
	err = r.log.decode(decoder)
	if err != nil {
		return err
	}
	return nil
}
