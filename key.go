package serial

import "crypto/sha512"

type StrPointer struct {
	StrId uint64
	Pointer
}

// Key is the representation of a key/value pair in a database
type KeyVal struct {
	KeyPointer, ValuePointer StrPointer // points to the string in question
	Key, Value               []byte
}

// BinSize returns the size once encoded as binary
func (k *KeyVal) BinSize() uint64 {
	return k.KeyPointer.BinSize() + k.ValuePointer.BinSize()
}

// KeyKey creates a 64 byte hash of the value
func (k KeyVal) GenKey() []byte {
	return Hash(k.Value)
}

// HashKey creates a 64 byte hash of the given key
func Hash(b []byte) []byte {
	x := sha512.Sum512(b)
	return x[:]
}
