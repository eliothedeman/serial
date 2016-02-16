package serial

import (
	"testing"

	"github.com/eliothedeman/randutil"
)

func TestMarshalUnmarshalPointer(t *testing.T) {
	for i := 0; i < 1000; i++ {
		p := NewPointer(randutil.Uint64(), randutil.Uint64(), 0, now())

		buff := p.MarshalTable(nil)

		n := NewPointer(0, 0, 0, now())
		err := n.UnmarshalTable(buff)
		if err != nil {
			t.Error(err)
		}

		if *n != *p {
			t.Fatal()
		}
	}
}

func TestPointerFlag(t *testing.T) {
	p := NewPointer(0, 0, FlagValid, now())

	if !p.HasFlag(FlagValid) {
		t.Fail()
	}

	if p.HasFlag(FlagMarkedForDeletion) {
		t.Fail()
	}
}

func TestAddFlag(t *testing.T) {
	p := NewPointer(0, 0, FlagValid, now())

	if p.HasFlag(FlagMarkedForDeletion) {
		t.Fatal()
	}

	p.AddFlag(FlagMarkedForDeletion)
	if !p.HasFlag(FlagMarkedForDeletion) {
		t.Fatal()
	}

}

func TestRemoveFlag(t *testing.T) {
	p := NewPointer(0, 0, FlagValid, now())

	if !p.HasFlag(FlagValid) {
		t.Fail()
	}
	p.RemoveFlag(FlagValid)

	if p.HasFlag(FlagValid) {
		t.Fail()
	}

}
