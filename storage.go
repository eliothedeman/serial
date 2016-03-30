package serial

import "io"

// Storage has all the methods for reading and writing to a datastore.
type Storage interface {
	io.Reader
	io.Writer
	io.Closer
	io.Seeker
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

// ReadData read the data from storage that the pointer points to.
func ReadData(s io.ReadSeeker, p Pointer) ([]byte, error) {
	// seek to the head of the pointer
	s.Seek(int64(p.Addr), 0)

	// create  buffer large enough for the read value
	buff := make([]byte, p.Size)

	// read the buffer from the storage file
	err := readFull(s, buff)
	return buff, err
}

// WriteData given a storage and a buffer, write the data to the storage and return a pointer to that data
func WriteData(s io.WriteSeeker, b []byte) (Pointer, error) {

	// seek to the end of the storage
	offset, err := s.Seek(0, 2)
	if err != nil {
		return Pointer{}, err
	}

	// write the buffer to storage
	err = writeFull(s, b)
	if err != nil {
		return Pointer{}, err
	}

	return Pointer{
		Addr: uint64(offset),
		Size: uint64(len(b)),
	}, nil
}
