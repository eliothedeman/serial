package serial

import (
	"bytes"
	"fmt"
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
	s, _ := os.Create(fmt.Sprintf("str.db.%d", id))
	p, _ := os.Create(fmt.Sprintf("ptr.db.%d", id))
	m, _ := os.Create(fmt.Sprintf("meta.db.%d", id))

	d := NewDB(p, s, m)
	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in f", r)
		}
		// clean up
		s.Close()
		p.Close()
		m.Close()
		os.Remove(s.Name())
		os.Remove(p.Name())
		os.Remove(m.Name())
	}()

	f(d)

}

func TestPutStrWithCache(t *testing.T) {
	runWithDb(func(db *DB) {
		ptr, err := db.PutKeyVal([]byte("hello"), []byte("world"))
		if err != nil {
			t.Error(err)
		}

		buff, err := db.getStr(&ptr.KeyPointer)
		if err != nil {
			t.Error(err)
		}

		if string(buff) != "hello" {
			t.Fail()
		}
	})
}

func TestPutStrWithoutCache(t *testing.T) {
	runWithDb(func(db *DB) {
		ptr, err := db.PutKeyVal([]byte("hello"), []byte("world"))
		if err != nil {
			t.Error(err)
		}

		// kill the cache
		db.idToStr = make(map[uint64][]byte)

		buff, err := db.getStr(&ptr.KeyPointer)
		if err != nil {
			t.Error(err)
		}

		if string(buff) != "hello" {
			t.Fail()
		}
	})
}

func BenchmarkPutStr(b *testing.B) {

	strs := make([][]byte, b.N)
	for i := 0; i < len(strs); i++ {
		strs[i] = []byte(randutil.AlphaString(randutil.IntRange(10, 10)))
	}

	runWithDb(func(db *DB) {
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			db.putStr(strs[i%len(strs)])

		}

	})
}
