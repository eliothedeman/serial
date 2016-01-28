package serial

import (
	"io"
	"sync/atomic"

	"github.com/tchap/go-patricia/patricia"
)

type Storage interface {
	io.Reader
	io.Writer
	io.Closer
	io.Seeker
}

// Returns the size of a value once it is encoded as binary
type BinSizer interface {
	BinSize() uint64
}

func readFull(r io.Reader, buff []byte) error {
	x := len(buff)
	y := 0
	for y < x {
		n, err := r.Read(buff[y:])
		if err != nil {
			return err
		}

		y += n
	}

	return nil
}

func writeFull(w io.Writer, buff []byte) error {
	x := len(buff)
	y := 0

	for y < x {
		n, err := w.Write(buff[y:])
		if err != nil {
			return err
		}

		y += n
	}

	return nil
}

// ReadData read the data from storage that the pointer points to
func ReadData(s io.ReadSeeker, p *Pointer) ([]byte, error) {
	// seek to the head of the pointer
	s.Seek(int64(p.head), 0)

	// create  buffer large enough for the read value
	buff := make([]byte, p.size)

	// read the buffer from the storage file
	err := readFull(s, buff)
	return buff, err
}

// WriteData given a storage and a buffer, write the data to the storage and return a pointer to that data
func WriteData(s io.WriteSeeker, b []byte) (*Pointer, error) {

	// seek to the end of the storage
	offset, err := s.Seek(0, 2)
	if err != nil {
		return nil, err
	}

	// write the buffer to storage
	err = writeFull(s, b)
	if err != nil {
		return nil, err
	}

	// create a new pointer that points to the data that has just been written
	p := NewPointer(uint64(offset), uint64(len(b)))
	return p, nil
}

// Db is a view into a database
type DB struct {
	pointerStor, strStor, metaStor Storage
	strTrie                        *patricia.Trie
	idToStr                        map[uint64][]byte
	strCount                       uint64
}

// NewDB creates and returns a new DB
func NewDB(pointerStor, strStor, metaStor Storage) *DB {
	db := &DB{
		pointerStor: pointerStor,
		strStor:     strStor,
		metaStor:    metaStor,
		strTrie:     patricia.NewTrie(),
		idToStr:     make(map[uint64][]byte),
	}

	return db
}

// Close closes all open databases
func (d *DB) Close() error {
	var vErr error
	err := d.pointerStor.Close()
	if err != nil {
		vErr = err
	}

	err = d.strStor.Close()
	if err != nil {
		vErr = err
	}

	err = d.metaStor.Close()
	if err != nil {
		vErr = err
	}

	return vErr
}

// putStr inserts a string into the string store and returns a pointer to it's location
func (d *DB) putStr(str []byte) (*StrPointer, error) {

	// check to see if this string has already been inserted into the database
	if v := d.strTrie.Get(patricia.Prefix(str)); v != nil {
		return v.(*StrPointer), nil
	}

	// create an id for the string
	id := atomic.AddUint64(&d.strCount, 1)

	// put the str into the database
	ptr, err := WriteData(d.strStor, str)
	if err != nil {
		return nil, err
	}

	strPtr := &StrPointer{}
	strPtr.Pointer = *ptr
	strPtr.StrId = id

	// insert this into the trie cache
	d.strTrie.Insert(patricia.Prefix(str), strPtr)

	return strPtr, nil

}

// getStr given a pointer return the string that it points to
func (d *DB) getStr(ptr *StrPointer) ([]byte, error) {

	// check the cache for this string
	buff, ok := d.idToStr[ptr.StrId]
	if ok {
		return buff, nil
	}

	// read the buffer from storage
	buff, err := ReadData(d.strStor, &ptr.Pointer)
	if err != nil {
		return nil, err
	}

	// insert the buffer into the cache
	d.idToStr[ptr.StrId] = buff
	d.strTrie.Insert(patricia.Prefix(buff), ptr)

	return buff, nil
}

// strFromId given a id look up the string that this id points to. Nil if not found
func (d *DB) strFromId(id uint64) []byte {
	b, ok := d.idToStr[id]
	if !ok {
		return nil
	}

	return b
}

// PutKeyVal inserts a new key and value into the database and returns a new KeyVal pointer
func (d *DB) PutKeyVal(key, val []byte) (*KeyVal, error) {
	kv := KeyVal{}

	kv.Key = key
	kv.Value = val

	// write the key and value to the str db
	ptr, err := d.putStr(key)
	if err != nil {
		return nil, err
	}

	kv.KeyPointer = *ptr

	// write the val
	ptr, err = d.putStr(val)
	if err != nil {
		return nil, err
	}

	kv.ValuePointer = *ptr

	return &kv, nil
}

// InsertPoint appends the given point to the database
func (d *DB) InsertPoint(p *Point) (*TimedPointer, error) {
	return nil, nil

}
