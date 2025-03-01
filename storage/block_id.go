package storage

import (
	"fmt"
	"hash/fnv"
)

type BlockId interface {
	Filename() string
	Number() uint64
	Equals(other BlockId) bool
	ToString() string
	HashCode() int
}

func NewBlockId(filename string, number uint64) (BlockId, error) {
	return &localblock{filename, number}, nil
}

type localblock struct {
	filename string
	number   uint64
}

func (b *localblock) Filename() string {
	return b.filename
}

func (b *localblock) Number() uint64 {
	return b.number
}

func (b *localblock) Equals(other BlockId) bool {
	return b.Filename() == other.Filename() && b.Number() == other.Number()
}

func (b *localblock) ToString() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename(), b.Number())
}

func (b *localblock) HashCode() int {
	str := b.ToString()
	h := fnv.New32a()
	h.Write([]byte(str))
	return int(h.Sum32())
}
