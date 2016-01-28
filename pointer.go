package serial

import (
	"encoding/binary"
	"errors"
	"time"
)

var (
	IncorrectBufferSize = errors.New("Incorrect buffer size")
)

// TimedPointer is a pointer that referes to a spesific point in time
type TimedPointer struct {
	TimeStamp time.Time
	Pointer
}

// NewTimedPointer creats and returns a new TimedPointer
func NewTimedPointer(head, size uint64, t time.Time) *TimedPointer {
	return &TimedPointer{
		t, Pointer{head, size},
	}
}

// MarshalDB encodes a TimedPointer as binary
func (t *TimedPointer) MarshalDB() ([]byte, error) {

	// make a buffer big enough
	buff := make([]byte, t.BinSize())

	// marshal the timestamp
	timeBuff, err := t.TimeStamp.MarshalBinary()
	if err != nil {
		return buff, err
	}

	// copy the time buffer in
	copy(buff[:15], timeBuff)

	// copy the pointer buffer into the buffer
	ptrBuff, _ := t.Pointer.MarshalDB()
	copy(buff[15:], ptrBuff)

	return buff, nil
}

// UnmarshalDB decodes a binary blob into a timed pointer
func (t *TimedPointer) UnmarshalDB(buff []byte) error {
	if t.BinSize() != uint64(len(buff)) {
		return IncorrectBufferSize
	}

	// first 15 are the timestamp
	err := t.TimeStamp.UnmarshalBinary(buff[:15])
	if err != nil {
		return err
	}

	// the rest is from the pointer
	return t.Pointer.UnmarshalDB(buff[15:])
}

// BinSize returns the size of a TimedPointer once it is encoded as binary
func (t *TimedPointer) BinSize() uint64 {
	return t.Pointer.BinSize() + 15 // size of the pointer + the 15 bytes for the time
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
func (p *Pointer) MarshalDB() ([]byte, error) {
	buff := make([]byte, 16)

	binary.LittleEndian.PutUint64(buff[:8], p.head)
	binary.LittleEndian.PutUint64(buff[8:16], p.size)
	return buff, nil
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
