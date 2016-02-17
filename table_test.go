package serial

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"testing"

	. "github.com/smartystreets/goconvey/convey"

	"github.com/eliothedeman/randutil"
)

var (
	testCount uint64
)

func TestWriteFull(t *testing.T) {
	Convey("Given an empty buffer", t, func() {
		buff := bytes.NewBuffer(nil)

		Convey("When a random string is written into the buffer", func() {
			msg := randutil.AlphaString(randutil.IntRange(10, 100))
			err := writeFull(buff, []byte(msg))
			if err != nil {
				t.Error(err)
			}

			Convey("The next value in the buffer should be the same string we just wrote", func() {
				So(msg, ShouldEqual, string(buff.Next(len(msg))))
			})

		})

	})

}

func TestReadFull(t *testing.T) {
	buff := bytes.NewBuffer(nil)

	for i := 0; i < 10000; i++ {
		msg := randutil.AlphaString(randutil.IntRange(10, 100))
		err := writeFull(buff, []byte(msg))
		if err != nil {
			t.Error(err)
		}

		b := make([]byte, len(msg))
		err = readFull(buff, b)
		if err != nil {
			t.Error(err)
		}

		if msg != string(b) {
			t.Fatal(msg, b)
		}
	}

}

func runWithTable(f func(Table *Table)) {

	id := atomic.AddUint64(&testCount, 1)
	p, _ := os.Create(fmt.Sprintf("ptr.Table.%d", id))
	m, _ := os.Create(fmt.Sprintf("meta.Table.%d", id))

	d := NewTable(p, m)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
		// clean up
		p.Close()
		m.Close()
		os.Remove(p.Name())
		os.Remove(m.Name())
	}()

	f(d)

}

func TestReadWriteBlock(t *testing.T) {
	runWithTable(func(Table *Table) {
		blks := ranTablelocks(1000)

		for _, b := range blks {
			p, err := Table.WriteBlock(b)
			if err != nil {
				t.Fatal(err)
			}

			nb, err := Table.ReaTablelock(p)
			if err != nil {
				t.Fatal(err)
			}

			if !nb.Equals(b) {
				t.Fail()
			}
		}
	})
}

func BenchmarkWriteBlock(b *testing.B) {
	runWithTable(func(Table *Table) {
		blks := ranTablelocks(1000)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Table.WriteBlock(blks[i%len(blks)])
		}
	})
}

func BenchmarkReaTablelockSequential(b *testing.B) {
	runWithTable(func(Table *Table) {
		blks := ranTablelocks(1000)
		pointers := make([]*Pointer, b.N)

		// write down enough blocks for us to read in series
		for i := 0; i < b.N; i++ {
			p, _ := Table.WriteBlock(blks[i%len(blks)])
			pointers[i] = p
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			Table.ReaTablelock(pointers[i])
		}
	})
}

func TestStreamPointersBetween(t *testing.T) {
	runWithTable(func(Table *Table) {
		blks := ranTablelocks(1000)

		// write a bunch of blocks
		for _, b := range blks {
			p, err := Table.WriteBlock(b)
			if err != nil {
				t.Fatal(err)
			}

			nb, err := Table.ReaTablelock(p)
			if err != nil {
				t.Fatal(err)
			}

			if !nb.Equals(b) {
				t.Fail()
			}
		}

		// read all dem pointers
		pc, ec := Table.streamPointersBetween(0, math.MaxUint64)
		i := 0
		for range pc {
			i++
		}
		if i != 1000 {
			t.Fatal(i)
		}

		err := <-ec
		if err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkStreamPointerBetween(b *testing.B) {
	runWithTable(func(Table *Table) {
		blks := ranTablelocks(1000)

		// write a bunch of blocks
		for _, blk := range blks {
			Table.WriteBlock(blk)
		}

		b.ResetTimer()
		b.ReportAllocs()

		// read all dem pointers
		for i := 0; i < b.N/len(blks); i++ {
			pc, ec := Table.streamPointersBetween(0, math.MaxUint64)
			for range pc {
			}
			err := <-ec
			if err != nil {
				b.Fatal(err)
			}
		}

	})
}

func TestStreamBlocksBetween(t *testing.T) {
	runWithTable(func(Table *Table) {
		blks := ranTablelocks(1000)

		// write a bunch of blocks
		for _, b := range blks {
			p, err := Table.WriteBlock(b)
			if err != nil {
				t.Fatal(err)
			}

			nb, err := Table.ReaTablelock(p)
			if err != nil {
				t.Fatal(err)
			}

			if !nb.Equals(b) {
				t.Fail()
			}
		}

		// read all dem blocks
		bc, ec := Table.StreamBlocksBetween(0, math.MaxUint64)
		i := 0
		for range bc {
			i++
		}
		if i != 1000 {
			t.Fatal(i)
		}

		err := <-ec
		if err != nil {
			t.Fatal(err)
		}
	})
}

func BenchmarkStreamBlockBetween(b *testing.B) {
	runWithTable(func(Table *Table) {
		blks := ranTablelocks(1000)

		// write a bunch of blocks
		for _, blk := range blks {
			Table.WriteBlock(blk)
		}

		b.ResetTimer()
		b.ReportAllocs()

		// read all dem pointers
		for i := 0; i < b.N/len(blks); i++ {
			bc, ec := Table.StreamBlocksBetween(0, math.MaxUint64)
			for range bc {
			}
			err := <-ec
			if err != nil {
				b.Fatal(err)
			}
		}

	})
}
