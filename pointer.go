package serial

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
	"unsafe"

	"github.com/eliothedeman/immut"
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
	head, size, flags, insertTime uint64
}

// NewPointer creats and returns a new Pointer object
func NewPointer(head, size, flags, insertTime uint64) *Pointer {
	return &Pointer{
		head, size, flags, insertTime,
	}
}

// BinSize returns the size of the binary encoded pointer
func (p *Pointer) BinSize() uint64 {
	// head + size + flags
	return 32
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
	binary.LittleEndian.PutUint64(buff[24:32], p.insertTime)
	return buff
}

// UnmarhsalDB decodes binary into a pointer
func (p *Pointer) UnmarhsalDB(buff []byte) error {

	if uint64(len(buff)) != p.BinSize() {
		return IncorrectBufferSize
	}

	p.head = binary.LittleEndian.Uint64(buff[:8])
	p.size = binary.LittleEndian.Uint64(buff[8:16])
	p.flags = binary.LittleEndian.Uint64(buff[16:24])
	p.insertTime = binary.LittleEndian.Uint64(buff[24:32])

	return nil
}

// pointerStore is a threadsafe access to the view of pointers at a given time
type pointerStore struct {
	storePtr   uintptr
	dummyStore *immut.UintHashMap // only here for gc reasons
}

func newPointerStore() *pointerStore {
	h := immut.NewUintHashMap()
	s := &pointerStore{}
	s.setStore(h)
	return s
}

func (p *pointerStore) getStore() *immut.UintHashMap {

	// get the current pointer atomically
	addr := atomic.LoadUintptr(&p.storePtr)

	// return the hash map at the location
	return (*immut.UintHashMap)(unsafe.Pointer(addr))
}

func (p *pointerStore) setStore(s *immut.UintHashMap) {
	addr := uintptr(unsafe.Pointer(s))
	atomic.StoreUintptr(&p.storePtr, addr)

	// set the dummy store just for gc sake
	p.dummyStore = s
}

// put inserts a value into the pointer store at the given key
func (p *pointerStore) put(k uint64, v *Pointer) {
	store := p.getStore().Put(k, v)

	p.setStore(store)
}

func (p *pointerStore) get(k uint64) *Pointer {
	store := p.getStore()
	v, ok := store.Get(k)
	if !ok {
		return nil
	}
	return v.(*Pointer)
}
