package serial

// A Pointer provides the address and size of data
type Pointer struct {
	Addr,
	Size uint64
}

func (p Pointer) isNil() bool {
	return p.Addr != 0
}
