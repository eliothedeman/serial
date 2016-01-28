package serial

import (
	"testing"

	"github.com/eliothedeman/randutil"
)

func TestMarshalUnmarshalPointer(t *testing.T) {
	for i := 0; i < 1000; i++ {
		p := NewPointer(randutil.Uint64(), randutil.Uint64())

		buff := p.MarshalDB()

		n := NewPointer(0, 0)
		err := n.UnmarshalDB(buff)
		if err != nil {
			t.Error(err)
		}

		if *n != *p {
			t.Fatal()
		}
	}
}

func TestMarshalUnmarshalTimedPointer(t *testing.T) {

	for i := 0; i < 1000; i++ {
		p := NewTimedPointer(randutil.Uint64(), randutil.Uint64(), randutil.Uint64())

		buff := p.MarshalDB()

		np := NewTimedPointer(0, 0, 0)

		err := np.UnmarshalDB(buff)
		if err != nil {
			t.Fatal(err)
		}

		if *p != *np {
			t.Fatal(*p, *np)
		}
	}
}

func BenchmarkMarshalTimedPointer(b *testing.B) {
	p := NewTimedPointer(0, 0, randutil.Uint64())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		p.MarshalDB()
	}
}

func BenchmarkUnmarshalTimedPointer(b *testing.B) {
	p := NewTimedPointer(0, 0, randutil.Uint64())
	buff := p.MarshalDB()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		p.UnmarshalDB(buff)
	}
}
