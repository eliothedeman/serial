package serial

// Point is a single point in the database
type Point struct {
	InsertTime uint64
	Data       []KeyVal
}

// Size returns the size of a point once it has been encoded as binary
func (p *Point) BinSize() uint64 {
	// time + (size of data pointers * num pointers)
	if len(p.Data) == 0 {
		return 8
	}

	return 8 + (p.Data[0].BinSize() * uint64(len(p.Data)))
}

// MarshalDB encodes the point to a form which it can be stored in the db
func (p *Point) MarshalDB() []byte {

	buff := make([]byte, p.BinSize())

	var offset uint64
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
