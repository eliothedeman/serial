package serial

import (
	"testing"

	"github.com/eliothedeman/randutil"
)

func randBlock() *Block {
	iTime := randutil.Uint64()
	kvs := randKVs(randutil.IntRange(2, 15))
	return NewBlock(iTime, kvs)
}

func randBlocks(n int) []*Block {
	blocks := make([]*Block, n)
	for i := 0; i < n; i++ {
		blocks[i] = randBlock()
	}
	return blocks
}

func TestBlockMarshalUnmarshal(t *testing.T) {
	b := randBlock()
	buff := b.MarshalDB(nil)
	n := &Block{}
	err := n.UnmarshalDB(buff)
	if err != nil {
		t.Error(err)
	}

	if !b.Equals(n) {
		t.Fail()
	}
}

func TestBlockMarshalUnmarshalPreAlloc(t *testing.T) {
	b := randBlock()
	buff := b.MarshalDB(make([]byte, 1000))
	n := &Block{}
	err := n.UnmarshalDB(buff)
	if err != nil {
		t.Error(err)
	}

	if !b.Equals(n) {
		t.Fail()
	}
}

func BenchmarkBlockMarshal(b *testing.B) {
	blocks := randBlocks(1000)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		blocks[i%len(blocks)].MarshalDB(nil)
	}
}

func BenchmarkBlockMarshalPreAlloc(b *testing.B) {
	blocks := randBlocks(1000)

	b.ResetTimer()
	b.ReportAllocs()
	buff := make([]byte, 10000)
	for i := 0; i < b.N; i++ {
		buff = blocks[i%len(blocks)].MarshalDB(buff)
	}
}

func BenchmarkBlockUnmarshal(b *testing.B) {
	blocks := randBlocks(1000)
	buffs := make([][]byte, len(blocks))

	// fill the buffs
	for i, block := range blocks {
		buffs[i] = block.MarshalDB(nil)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		blocks[i%len(blocks)].UnmarshalDB(buffs[i%len(buffs)])
	}

}
