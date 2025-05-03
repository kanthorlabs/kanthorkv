package storage

import (
	"fmt"
	"hash/fnv"
)

// BlockId represents a specific block in a specific file
type BlockId struct {
	filename string
	blknum   int
}

// NewBlockId creates a new BlockId with the given filename and block number
func NewBlockId(filename string, blknum int) (*BlockId, error) {
	if filename == "" {
		return nil, ErrBlockIdFilenameEmpty()
	}

	if blknum < 0 {
		return nil, ErrBlockIdInvalidBlockNumber(blknum)
	}

	return &BlockId{filename: filename, blknum: blknum}, nil
}

// Filename returns the name of the file this block belongs to
func (b *BlockId) Filename() string {
	return b.filename
}

// Number returns the block number within the file
func (b *BlockId) Number() int {
	return b.blknum
}

// String returns a string representation of the BlockId
func (b *BlockId) String() string {
	return fmt.Sprintf("[file:%s, block:%d]", b.filename, b.blknum)
}

// Equals checks if two BlockIds represent the same block
func (b *BlockId) Equals(other *BlockId) bool {
	return b.filename == other.filename && b.blknum == other.blknum
}

func (b *BlockId) ToString() string {
	return fmt.Sprintf("[file %s, block %d]", b.Filename(), b.Number())
}

func (b *BlockId) HashCode() int {
	str := b.ToString()
	h := fnv.New32a()
	h.Write([]byte(str))
	return int(h.Sum32())
}
