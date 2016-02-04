package serial

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"testing"

	"github.com/eliothedeman/randutil"
)

var (
	testCount uint64
)

func TestWriteFull(t *testing.T) {
	buff := bytes.NewBuffer(nil)

	for i := 0; i < 10000; i++ {
		msg := randutil.AlphaString(randutil.IntRange(10, 100))
		err := writeFull(buff, []byte(msg))
		if err != nil {
			t.Error(err)
		}

		if msg != string(buff.Next(len(msg))) {
			t.Fatal()
		}
	}
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

func runWithDb(f func(db *DB)) {

	id := atomic.AddUint64(&testCount, 1)
	p, _ := os.Create(fmt.Sprintf("ptr.db.%d", id))
	m, _ := os.Create(fmt.Sprintf("meta.db.%d", id))

	d := NewDB(p, m)
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
	runWithDb(func(db *DB) {
		blks := randBlocks(1000)

		for _, b := range blks {
			p, err := db.WriteBlock(b)
			if err != nil {
				t.Fatal(err)
			}

			nb, err := db.ReadBlock(p)
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
	runWithDb(func(db *DB) {
		blks := randBlocks(1000)

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			db.WriteBlock(blks[i%len(blks)])
		}
	})
}

func BenchmarkReadBlockSequential(b *testing.B) {
	runWithDb(func(db *DB) {
		blks := randBlocks(1000)
		pointers := make([]*Pointer, b.N)

		// write down enough blocks for us to read in series
		for i := 0; i < b.N; i++ {
			p, _ := db.WriteBlock(blks[i%len(blks)])
			pointers[i] = p
		}

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			db.ReadBlock(pointers[i])
		}
	})
}

func TestStreamPointersBetween(t *testing.T) {
	runWithDb(func(db *DB) {
		blks := randBlocks(1000)

		// write a bunch of blocks
		for _, b := range blks {
			p, err := db.WriteBlock(b)
			if err != nil {
				t.Fatal(err)
			}

			nb, err := db.ReadBlock(p)
			if err != nil {
				t.Fatal(err)
			}

			if !nb.Equals(b) {
				t.Fail()
			}
		}

		// read all dem pointers
		pc, ec := db.streamPointersBetween(0, math.MaxUint64)
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
	runWithDb(func(db *DB) {
		blks := randBlocks(1000)

		// write a bunch of blocks
		for _, blk := range blks {
			db.WriteBlock(blk)
		}

		b.ResetTimer()
		b.ReportAllocs()

		// read all dem pointers
		for i := 0; i < b.N/len(blks); i++ {
			pc, ec := db.streamPointersBetween(0, math.MaxUint64)
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
	runWithDb(func(db *DB) {
		blks := randBlocks(1000)

		// write a bunch of blocks
		for _, b := range blks {
			p, err := db.WriteBlock(b)
			if err != nil {
				t.Fatal(err)
			}

			nb, err := db.ReadBlock(p)
			if err != nil {
				t.Fatal(err)
			}

			if !nb.Equals(b) {
				t.Fail()
			}
		}

		// read all dem blocks
		bc, ec := db.StreamBlocksBetween(0, math.MaxUint64)
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
	runWithDb(func(db *DB) {
		blks := randBlocks(1000)

		// write a bunch of blocks
		for _, blk := range blks {
			db.WriteBlock(blk)
		}

		b.ResetTimer()
		b.ReportAllocs()

		// read all dem pointers
		for i := 0; i < b.N/len(blks); i++ {
			bc, ec := db.StreamBlocksBetween(0, math.MaxUint64)
			for range bc {
			}
			err := <-ec
			if err != nil {
				b.Fatal(err)
			}
		}

	})
}
