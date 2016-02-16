package serial

import "encoding/binary"

// Block is the basic representation of a datapoint on disk
type Block struct {
	InsertTime uint64
	Data       []*KeyVal
}

// NewBlock Create and return a new block
func NewBlock(insertTime uint64, data []*KeyVal) *Block {
	return &Block{
		InsertTime: insertTime,
		Data:       data,
	}
}

// Equals returns true if two blocks are "value" equivalent
func (b *Block) Equals(n *Block) bool {
	if b.InsertTime != n.InsertTime {
		return false
	}

	if len(b.Data) != len(n.Data) {
		return false
	}

	for i := 0; i < len(b.Data); i++ {
		if !b.Data[i].Equals(n.Data[i]) {
			return false
		}
	}

	return true
}

// BinSize returns the size of a block when it is encoded as binary
func (b *Block) BinSize() uint64 {
	// size + insertTime + KeyVal count + KeyVals
	size := uint64(24)
	for i := 0; i < len(b.Data); i++ {
		size += b.Data[i].BinSize()
	}

	return size
}

// MarshalDB encodes a Block in the form it will be stored in the database
func (b *Block) MarshalDB(buff []byte) []byte {
	if buff == nil || len(buff) < int(b.BinSize()) {
		buff = make([]byte, b.BinSize())
	}

	// total size
	offset := uint64(0)
	binary.LittleEndian.PutUint64(buff[offset:offset+8], b.BinSize())
	offset += 8

	// insertTime
	binary.LittleEndian.PutUint64(buff[offset:offset+8], b.InsertTime)
	offset += 8

	// keyvalCount
	binary.LittleEndian.PutUint64(buff[offset:offset+8], uint64(len(b.Data)))
	offset += 8

	// fill up the reset of the buffer with the key val pairs
	for _, kv := range b.Data {

		// this will do it in place with out any new allocations
		kv.MarshalDB(buff[offset : offset+kv.BinSize()])
		offset += kv.BinSize()
	}

	return buff
}

// UnmarhsalDB decodes a Block from the form it was stored in the database
func (b *Block) UnmarhsalDB(buff []byte) error {
	size := binary.LittleEndian.Uint64(buff[0:8])
	if uint64(len(buff)) != size {
		return IncorrectBufferSize
	}

	offset := uint64(8)

	// insertTime
	b.InsertTime = binary.LittleEndian.Uint64(buff[offset : offset+8])
	offset += 8

	// keyValCount
	kvCount := binary.LittleEndian.Uint64(buff[offset : offset+8])
	offset += 8
	b.Data = make([]*KeyVal, kvCount)

	for i := uint64(0); i < kvCount; i++ {
		// make a new kv
		kv := &KeyVal{}

		// read the next size
		// the first 8 bytes of a KeyVal are it's binary size, so read that first and include it in the buffer passed for unmarshaling
		size = binary.LittleEndian.Uint64(buff[offset : offset+8])
		err := kv.UnmarhsalDB(buff[offset : offset+size])
		if err != nil {
			return err
		}

		// assign the new KeyVal
		b.Data[i] = kv
		offset += size
	}

	return nil
}
