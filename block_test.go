package serial

import (
	"testing"

	"github.com/eliothedeman/randutil"
)

func ranTablelock() *Block {
	iTime := randutil.Uint64()
	kvs := randKVs(randutil.IntRange(2, 15))
	return NewBlock(iTime, kvs)
}

func ranTablelocks(n int) []*Block {
	blocks := make([]*Block, n)
	for i := 0; i < n; i++ {
		blocks[i] = ranTablelock()
	}
	return blocks
}

func TestBlockMarshalUnmarshal(t *testing.T) {
	b := ranTablelock()
	buff := b.MarshalTable(nil)
	n := &Block{}
	err := n.UnmarshalTable(buff)
	if err != nil {
		t.Error(err)
	}

	if !b.Equals(n) {
		t.Fail()
	}
}

func TestBlockMarshalUnmarshalPreAlloc(t *testing.T) {
	b := ranTablelock()
	buff := b.MarshalTable(make([]byte, 1000))
	n := &Block{}
	err := n.UnmarshalTable(buff)
	if err != nil {
		t.Error(err)
	}

	if !b.Equals(n) {
		t.Fail()
	}
}

func BenchmarkBlockMarshal(b *testing.B) {
	blocks := ranTablelocks(1000)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		blocks[i%len(blocks)].MarshalTable(nil)
	}
}

func BenchmarkBlockMarshalPreAlloc(b *testing.B) {
	blocks := ranTablelocks(1000)

	b.ResetTimer()
	b.ReportAllocs()
	buff := make([]byte, 10000)
	for i := 0; i < b.N; i++ {
		buff = blocks[i%len(blocks)].MarshalTable(buff)
	}
}

func BenchmarkBlockUnmarshal(b *testing.B) {
	blocks := ranTablelocks(1000)
	buffs := make([][]byte, len(blocks))

	// fill the buffs
	for i, block := range blocks {
		buffs[i] = block.MarshalTable(nil)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		blocks[i%len(blocks)].UnmarshalTable(buffs[i%len(buffs)])
	}

}
