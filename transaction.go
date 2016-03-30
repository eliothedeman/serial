package serial

import (
	"encoding/binary"
	"io"
)

const (
	transactionRecordSize = 8 + 8 + 16 + 16
)

// A TransactionRecord is the base of the storage trie.
type TransactionRecord struct {
	ID     uint64
	Time   uint64
	Root   Pointer
	Parent Pointer
}

// MarshalBinary encodes a TransactionRecord into binary.
func (t *TransactionRecord) MarshalBinary() ([]byte, error) {
	b := make([]byte, transactionRecordSize)
	l := binary.LittleEndian

	l.PutUint64(b[0:8], t.ID)
	l.PutUint64(b[8:16], t.Time)
	l.PutUint64(b[16:24], t.Root.Addr)
	l.PutUint64(b[24:32], t.Root.Size)
	l.PutUint64(b[32:40], t.Parent.Addr)
	l.PutUint64(b[40:48], t.Parent.Size)

	return b, nil
}

// UnmarshalBinary decods a TransactionRecord from binary.
func (t *TransactionRecord) UnmarshalBinary(b []byte) error {
	if len(b) != transactionRecordSize {
		return ErrWrongSizeBuffer
	}

	l := binary.LittleEndian
	t.ID = l.Uint64(b[0:8])
	t.Time = l.Uint64(b[8:16])
	t.Root.Addr = l.Uint64(b[16:24])
	t.Root.Size = l.Uint64(b[24:32])
	t.Parent.Addr = l.Uint64(b[32:40])
	t.Parent.Size = l.Uint64(b[40:48])

	return nil
}

// A Tx is a transaction in a serial database
type Tx interface {
	Get(key []byte) []byte
	Put(key, value []byte)
	Delete(key []byte) []byte
	ID() uint64
}

// ROTx is a read only transaction.
type ROTx struct {
	id uint64
	s  io.ReadSeeker
	r  *Node
	errHandler
}

type errHandler struct {
	err error
}

// setError will set the first error that has been encounterd.
func (e errHandler) setError(err error) {
	if e.err == nil {
		e.err = err
	}
}

// Will return true if an error has been seen so far.
func (e errHandler) hasError() bool {
	return e.err != nil
}

// ID returns the id of this transaction.
func (r *ROTx) ID() uint64 {
	return r.id
}

// Get returns the key stored at the given value if it exists.
func (r *ROTx) Get(key []byte) []byte {
	if r.hasError() {
		return nil
	}
	b, err := get(key, r.r, r.s)
	if err != nil {
		return nil
	}

	return b
}

// Get the value at stored at the given key
func get(key []byte, root *Node, s io.ReadSeeker) ([]byte, error) {
	var depth uint64
	var err error
	k := hashKey(key)

	// if this part of the hash exists here, go deeper
	for root != nil {

		if root.test(key, s) {
			break
		}

		root, err = root.childAtIndex(indexAtDepth(k, depth), s)
		if err != nil {
			return nil, err
		}
	}

	return root.getRawValue(s)
}

// Put is a noop for a read only transaction.
func (r *ROTx) Put(key, value []byte) {
}

// Delete is a noop for a read only transaction.
func (r *ROTx) Delete(key []byte) []byte {
	return nil
}

// RWTx is a transaction with read and write functions.
type RWTx struct {
	id uint64
	s  io.ReadWriteSeeker
	r  *Node
	errHandler
}

// ID returns the id of this transaction.
func (r *RWTx) ID() uint64 {
	return r.id
}

// Put is a noop for a read only transaction.
func (r *RWTx) Put(key, value []byte) {
	if r.hasError() {
		return
	}
}

// Delete is a noop for a read only transaction.
func (r *RWTx) Delete(key []byte) []byte {
	if r.hasError() {
		return nil
	}

	return nil
}

// Get returns the value stored at the given key if it exists.
func (r *RWTx) Get(key []byte) []byte {
	if r.hasError() {
		return nil
	}
	b, err := get(key, r.r, r.s)
	if err != nil {
		return nil
	}

	return b
}
