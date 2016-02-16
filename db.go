package serial

import (
	"io"
	"log"
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

// Table is a view into a database
type Table struct {
	pointerStore, blockStore Storage
}

// NewTable creates and returns a new Table
func NewTable(pointerStore, blockStore Storage) *Table {
	Table := &Table{
		pointerStore: pointerStore,
		blockStore:   blockStore,
	}

	return Table
}

// Close closes all open databases
func (d *Table) Close() error {
	var vErr error
	err := d.pointerStore.Close()
	if err != nil {
		vErr = err
	}

	err = d.blockStore.Close()
	if err != nil {
		vErr = err
	}

	return vErr
}

// writeBlock appends a block to the blockStore
func (d *Table) writeBlock(b *Block) (*Pointer, error) {
	return WriteData(d.blockStore, b.MarshalDB(nil))
}

// writePointer appends a pointer
func (d *Table) writePointer(p *Pointer) error {
	return writeFull(d.pointerStore, p.MarshalDB(nil))
}

// WriteBlock appends a block to the blockstor and writes its pointer to the pointerstor
func (d *Table) WriteBlock(b *Block) (*Pointer, error) {

	// set the current time for the "insert time"
	b.InsertTime = uint64(time.Now().Unix())
	p, err := d.writeBlock(b)
	if err != nil {
		return p, err
	}

	return p, d.writePointer(p)
}

// ReaTablelock reads the block that is located at the given pointer
func (d *Table) ReaTablelock(p *Pointer) (*Block, error) {
	buff, err := ReadData(d.blockStore, p)
	if err != nil {
		return nil, err
	}

	b := &Block{}
	err = b.UnMarshalDB(buff)
	return b, err
}

// ReadPointer reads a pointer with the given index
func (d *Table) ReadPointer(index uint64) (*Pointer, error) {
	p := &Pointer{}

	// construct a pointer that will be used to read the pointer in question
	pp := Pointer{}
	pp.size = p.BinSize()
	pp.head = p.BinSize() * index

	// read out the buffer at the pointer location
	buff, err := ReadData(d.pointerStore, &pp)
	if err != nil {
		return nil, err
	}

	err = p.UnMarshalDB(buff)

	return p, err
}

// streamPointersBetween streams all pointers that lie between two points
func (d *Table) streamPointersBetween(start, end uint64) (chan *Pointer, chan error) {
	pc := make(chan *Pointer)
	ec := make(chan error)

	// handle errors by closing out channels and sending the error on
	handleError := func(err error) {
		close(pc)
		ec <- err
		close(ec)
	}
	handleNoError := func() {
		close(pc)
		ec <- nil
		close(ec)
	}
	go func() {
		p := &Pointer{}
		var err error
		var i uint64
		// start loading pointers until we see something that is > than start
		for p.insertTime < start {
			p, err = d.ReadPointer(i)

			if err != nil {
				if err == io.EOF {
					handleNoError()
					return
				}
				handleError(err)
				return
			}
			i++
		}

		// now that we are at the correct index, read all until we are at the end
		for p.insertTime < end {

			// send on the pointer
			if p.insertTime != 0 {
				pc <- p
			}

			// read the next pointer
			p, err = d.ReadPointer(i)

			if err != nil {
				if err == io.EOF {
					handleNoError()
					return
				}
				handleError(err)
				return
			}
			i++
		}
		handleNoError()
	}()

	return pc, ec
}

// StreamBlocksBetween starts streaming all blocks between the given timestamps
func (d *Table) StreamBlocksBetween(start, end uint64) (chan *Block, chan error) {
	bc := make(chan *Block)
	errChan := make(chan error)

	go func() {

		// start streaming the pointers
		pc, ec := d.streamPointersBetween(start, end)
		for p := range pc {

			// load the block at this pointer
			b, err := d.ReaTablelock(p)
			if err != nil {
				close(bc)
				errChan <- err
				close(errChan)

				// drain out the remaining pointers and error
				go func() {

					// drain the pointers
					for range pc {

					}

					// read the error if it exists
					err := <-ec
					if err != nil {

						// TODO : use logrus instead of built in log package
						log.Println(err)
					}
				}()
				return
			}

			bc <- b
		}
		close(bc)
		errChan <- <-ec
		close(errChan)
	}()

	return bc, errChan
}
