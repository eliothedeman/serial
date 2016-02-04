package serial

import (
	"io"
	"time"
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

type DBMarshaler interface {
	MarshalDB(buff []byte) []byte
}

type DBUnmarshaler interface {
	UnmarshalDB(buff []byte) error
}

type DBMarshalUnmarshaler interface {
	DBMarshaler
	DBUnmarshaler
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

// now returns the current unix timestamp as a uint64
func now() uint64 {
	return uint64(time.Now().Unix())
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
	p := NewPointer(uint64(offset), uint64(len(b)), FlagValid, now())
	return p, nil
}

// Db is a view into a database
type DB struct {
	pointerStor, blockStor Storage
}

// NewDB creates and returns a new DB
func NewDB(pointerStor, strStor, blockStor Storage) *DB {
	db := &DB{
		pointerStor: pointerStor,
		blockStor:   blockStor,
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

	err = d.blockStor.Close()
	if err != nil {
		vErr = err
	}

	return vErr
}

// writeBlock appends a block to the blockStor
func (d *DB) writeBlock(b *Block) (*Pointer, error) {
	return WriteData(d.blockStor, b.MarshalDB(nil))
}

// writePointer appends a pointer
func (d *DB) writePointer(p *Pointer) error {
	return writeFull(d.pointerStor, p.MarshalDB(nil))
}

// WriteBlock appends a block to the blockstor and writes its pointer to the pointerstor
func (d *DB) WriteBlock(b *Block) (*Pointer, error) {

	// set the current time for the "insert time"
	b.InsertTime = uint64(time.Now().Unix())
	p, err := d.writeBlock(b)
	if err != nil {
		return p, err
	}

	return p, d.writePointer(p)
}

// ReadBlock reads the block that is located at the given pointer
func (d *DB) ReadBlock(p *Pointer) (*Block, error) {
	buff, err := ReadData(d.blockStor, p)
	if err != nil {
		return nil, err
	}

	b := &Block{}
	err = b.UnmarshalDB(buff)
	return b, err
}
