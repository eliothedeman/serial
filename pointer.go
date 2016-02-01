package serial

import (
	"encoding/binary"
	"errors"
)

var (
	IncorrectBufferSize = errors.New("Incorrect buffer size")
)

const (
	FlagValid = 1 << iota
	FlagMarkedForDeletion
	FlagForignPointer
	FlagRedirect
)

// Pointer spesifies the location and size of a piece of data on disk
type Pointer struct {
	head, size, flags uint64
}

// NewPointer creats and returns a new Pointer object
func NewPointer(head, size, flags uint64) *Pointer {
	return &Pointer{
		head, size, flags,
	}
}

// BinSize returns the size of the binary encoded pointer
func (p *Pointer) BinSize() uint64 {
	// head + size + flags
	return 24
}

// HasFlag check to see if this pointer has the given flag
func (p *Pointer) HasFlag(flag uint64) bool {
	return p.flags&flag == flag
}

// AddFlag sets a bit flag on the pointer
func (p *Pointer) AddFlag(flag uint64) {
	p.flags = p.flags | flag
}

// RemoveFlag unsets a bit flag on the pointer
func (p *Pointer) RemoveFlag(flag uint64) {
	p.flags = p.flags ^ flag
}

// MarshalDB encodes a pointer as binary
func (p *Pointer) MarshalDB(buff []byte) []byte {
	if buff == nil || uint64(len(buff)) < p.BinSize() {
		buff = make([]byte, p.BinSize())
	}

	binary.LittleEndian.PutUint64(buff[:8], p.head)
	binary.LittleEndian.PutUint64(buff[8:16], p.size)
	binary.LittleEndian.PutUint64(buff[16:24], p.flags)
	return buff
}

// UnmarshalDB decodes binary into a pointer
func (p *Pointer) UnmarshalDB(buff []byte) error {

	if uint64(len(buff)) != p.BinSize() {
		return IncorrectBufferSize
	}

	p.head = binary.LittleEndian.Uint64(buff[:8])
	p.size = binary.LittleEndian.Uint64(buff[8:16])
	p.flags = binary.LittleEndian.Uint64(buff[16:24])

	return nil
}
