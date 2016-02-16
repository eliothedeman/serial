package serial

import (
	"bytes"
	"encoding/binary"
)

type StrPointer struct {
	StrId uint64
	Pointer
}

// Key is the representation of a key/value pair in a database
type KeyVal struct {
	Key, Value []byte
}

// NewKeyVal creates and returns a new key val
func NewKeyval(k, v []byte) *KeyVal {
	return &KeyVal{
		Key:   k,
		Value: v,
	}
}

// BinSize returns the size once encoded as binary
func (k *KeyVal) BinSize() uint64 {
	// header size + lenth of key and value
	return 24 + uint64(len(k.Key)) + uint64(len(k.Value))
}

// Equals two KeyVals for equality
func (k *KeyVal) Equals(kv *KeyVal) bool {
	if !bytes.Equal(k.Key, kv.Key) {
		return false
	}
	return bytes.Equal(k.Value, kv.Value)
}

// MarshalDB encodes a KeyVal pair in the form it will be stored in the database
func (k *KeyVal) MarshalDB(buff []byte) []byte {
	if buff == nil || len(buff) < int(k.BinSize()) {
		buff = make([]byte, k.BinSize())
	}

	// whole size header
	binary.LittleEndian.PutUint64(buff[0:8], k.BinSize())
	offset := 8

	// key size header
	kSize := len(k.Key)
	binary.LittleEndian.PutUint64(buff[offset:offset+8], uint64(kSize))
	offset += 8

	// key data
	copy(buff[offset:offset+kSize], k.Key)
	offset += kSize

	// val size header
	vSize := len(k.Value)
	binary.LittleEndian.PutUint64(buff[offset:offset+8], uint64(vSize))
	offset += 8

	copy(buff[offset:offset+vSize], k.Value)

	return buff[0:k.BinSize()]
}

// UnmarhsalDB decodes a KeyVal pair from its stabase format
func (k *KeyVal) UnmarhsalDB(buff []byte) error {

	// read the size header
	size := binary.LittleEndian.Uint64(buff[0:8])
	if uint64(len(buff)) != size {
		return IncorrectBufferSize
	}

	offset := uint64(8)

	// read key size
	size = binary.LittleEndian.Uint64(buff[offset : offset+8])
	offset += 8

	// pull out the key
	k.Key = buff[offset : offset+size]
	offset += size

	// read val size
	size = binary.LittleEndian.Uint64(buff[offset : offset+8])
	offset += 8

	k.Value = buff[offset : offset+size]

	return nil
}
