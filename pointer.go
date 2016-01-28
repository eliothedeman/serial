package serial

import (
	"encoding/binary"
	"errors"
)

var (
	IncorrectBufferSize = errors.New("Incorrect buffer size")
)

// TimedPointer is a pointer that referes to a spesific point in time
type TimedPointer struct {
	InsertTime uint64
	Pointer
}

// NewTimedPointer creats and returns a new TimedPointer
func NewTimedPointer(head, size, t uint64) *TimedPointer {
	return &TimedPointer{
		t, Pointer{head, size},
	}
}

// MarshalDB encodes a TimedPointer as binary
func (t *TimedPointer) MarshalDB() []byte {

	// make a buffer big enough
	buff := make([]byte, t.BinSize())

	// copy the time buffer in
	binary.LittleEndian.PutUint64(buff[:8], t.InsertTime)

	// copy the pointer buffer into the buffer
	copy(buff[8:], t.Pointer.MarshalDB())

	return buff
}

// UnmarshalDB decodes a binary blob into a timed pointer
func (t *TimedPointer) UnmarshalDB(buff []byte) error {
	if t.BinSize() != uint64(len(buff)) {
		return IncorrectBufferSize
	}

	// first 15 are the timestamp
	t.InsertTime = binary.LittleEndian.Uint64(buff[:8])

	// the rest is from the pointer
	return t.Pointer.UnmarshalDB(buff[8:])
}

// BinSize returns the size of a TimedPointer once it is encoded as binary
func (t *TimedPointer) BinSize() uint64 {
	return t.Pointer.BinSize() + 8 // size of the pointer + the 8 bytes for the time
}

// Pointer spesifies the location and size of a piece of data on disk
type Pointer struct {
	head, size uint64
}

// NewPointer creats and returns a new Pointer object
func NewPointer(head, size uint64) *Pointer {
	return &Pointer{
		head, size,
	}
}

func (p *Pointer) BinSize() uint64 {
	return 16
}

// MarshalDB encodes a pointer as binary
func (p *Pointer) MarshalDB() []byte {
	buff := make([]byte, 16)

	binary.LittleEndian.PutUint64(buff[:8], p.head)
	binary.LittleEndian.PutUint64(buff[8:16], p.size)
	return buff
}

// UnmarshalDB decodes binary into a pointer
func (p *Pointer) UnmarshalDB(buff []byte) error {

	if len(buff) != 16 {
		return IncorrectBufferSize
	}

	p.head = binary.LittleEndian.Uint64(buff[:8])
	p.size = binary.LittleEndian.Uint64(buff[8:16])

	return nil
}
