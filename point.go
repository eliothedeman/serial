package serial

import "encoding/binary"

// Point is a single point in the database
type Point struct {
	InsertTime uint64
	Data       []KeyVal
}

// NewPoint returns a new Point
func NewPoint(insertTime uint64, data []KeyVal) *Point {
	return &Point{
		InsertTime: insertTime,
		Data:       data,
	}
}

// Size returns the size of a point once it has been encoded as binary
func (p *Point) BinSize() uint64 {
	// time + num points(size of data pointers * num pointers)
	if len(p.Data) == 0 {
		return 8 + 8
	}

	return 8 + 8 + (p.Data[0].BinSize() * uint64(len(p.Data)))
}

// MarshalDB encodes the point to a form which it can be stored in the db
func (p *Point) MarshalDB() []byte {

	buff := make([]byte, p.BinSize())

	// write the timestamp
	binary.LittleEndian.PutUint64(buff[0:8], p.InsertTime)

	// number kv pairs
	binary.LittleEndian.PutUint64(buff[8:16], uint64(len(p.Data)))

	var offset uint64 = 16
	// copy in the pointers
	for _, kv := range p.Data {

		// copy into the buffers
		copy(buff[offset:offset+kv.KeyPointer.BinSize()], kv.KeyPointer.MarshalDB())
		offset += kv.KeyPointer.BinSize()
		copy(buff[offset:offset+kv.ValuePointer.BinSize()], kv.ValuePointer.MarshalDB())
		offset += kv.ValuePointer.BinSize()
	}

	return buff

}

// UnmarshalDB decodes a point from it's db representation
func (p *Point) UnmarshalDB(buff []byte) error {

	// make sure we at least have enough buffer to get the header
	if len(buff) < 16 {
		return IncorrectBufferSize
	}

	// insert time
	p.InsertTime = binary.LittleEndian.Uint64(buff[0:8])

	// count
	count := binary.LittleEndian.Uint64(buff[8:16])

	if uint64(len(buff)) < (16 + (count * 2 * NewPointer(0, 0).BinSize())) {
		return IncorrectBufferSize
	}

	// create a slice to whold all of the data
	p.Data = make([]KeyVal, count)

	var offset uint64 = 16
	var err error
	// make sure we have enough buffer for all of the pointers
	for i := uint64(0); i < count; i++ {
		kv := KeyVal{}

		// load the key pointer
		err = kv.KeyPointer.UnmarshalDB(buff[offset : offset+kv.KeyPointer.BinSize()])
		if err != nil {
			return err
		}
		offset += kv.ValuePointer.BinSize()
		err = kv.ValuePointer.UnmarshalDB(buff[offset : offset+kv.ValuePointer.BinSize()])
		if err != nil {
			return err
		}
		offset += kv.ValuePointer.BinSize()
	}

	return nil
}
