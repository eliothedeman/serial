package serial

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io"
)

const (
	bits              = 4
	width             = 1 << bits
	mask              = width - 1
	nodeMarshaledSize = 8 + 8 + 16 + 16 + (16 * width)
)

var (

	// ErrWrongSizeBuffer is returend if a buffer is provided that is not of the correct size.
	ErrWrongSizeBuffer = errors.New("invalid sized buffer")

	// ErrInvalidChildIndex is returend if a child index is out of range.
	ErrInvalidChildIndex = errors.New("invalid child index")
)

func hashKey(b []byte) uint64 {
	h := fnv.New64()
	h.Write(b)
	return h.Sum64()
}

// A Node is the on disk representation of a key value pair
type Node struct {
	Key           uint64
	TransactionID uint64
	RawKey        Pointer
	RawValue      Pointer
	Children      [width]Pointer
}

// indexAtDepth returns the index of a child node at the given depth.
func indexAtDepth(key, depth uint64) uint64 {
	return (key >> (depth)) & mask
}

// Test to see if this key is actually stored at this node
func (n *Node) test(key []byte, r io.ReadSeeker) bool {
	h := hashKey(key)
	if n.Key != h {
		return false
	}

	raw, err := n.getRawKey(r)
	if err != nil {
		return false
	}

	return bytes.Equal(raw, key)
}

// getRawValue will return the data stored at the pointer for the  raw value.
func (n *Node) getRawValue(r io.ReadSeeker) ([]byte, error) {
	return ReadData(r, n.RawValue)
}

// getRawKey will return the data stored at the pointer for the raw key.
func (n *Node) getRawKey(r io.ReadSeeker) ([]byte, error) {
	return ReadData(r, n.RawKey)
}

// childAtIndex returns the child node at the given index if it exists.
func (n *Node) childAtIndex(i uint64, r io.ReadSeeker) (*Node, error) {
	if i > width {
		return nil, ErrInvalidChildIndex
	}

	// No child with that index.
	if n.Children[i].Addr == 0 {
		return nil, nil
	}

	buff, err := ReadData(r, n.Children[i])
	if err != nil {
		return nil, err
	}

	x := &Node{}
	err = x.UnmarshalBinary(buff)

	return x, err
}

// MarshalBinary encodes the node as binary.
func (n *Node) MarshalBinary() ([]byte, error) {
	b := make([]byte, nodeMarshaledSize)

	l := binary.LittleEndian
	l.PutUint64(b[0:8], n.Key)
	l.PutUint64(b[8:16], n.TransactionID)
	l.PutUint64(b[16:24], n.RawKey.Addr)
	l.PutUint64(b[24:32], n.RawKey.Size)
	l.PutUint64(b[32:40], n.RawValue.Addr)
	l.PutUint64(b[40:48], n.RawValue.Size)
	for i := 0; i < width; i++ {
		l.PutUint64(b[48+(i*8):56+(i*8)], n.Children[i].Addr)
		l.PutUint64(b[56+(i*8):64+(i*8)], n.Children[i].Size)
	}

	return b, nil
}

// UnmarshalBinary decods the binary representation of a node.
func (n *Node) UnmarshalBinary(b []byte) error {
	if len(b) != nodeMarshaledSize {
		return ErrWrongSizeBuffer
	}

	l := binary.LittleEndian
	n.Key = l.Uint64(b[0:8])
	n.TransactionID = l.Uint64(b[8:16])
	n.RawKey.Addr = l.Uint64(b[16:24])
	n.RawKey.Size = l.Uint64(b[24:32])
	n.RawValue.Addr = l.Uint64(b[32:40])
	n.RawValue.Size = l.Uint64(b[40:48])
	for i := 0; i < width; i++ {
		n.Children[i].Addr = l.Uint64(b[48+(i*8) : 56+(i*8)])
		n.Children[i].Size = l.Uint64(b[56+(i*8) : 64+(i*8)])
	}

	return nil
}
