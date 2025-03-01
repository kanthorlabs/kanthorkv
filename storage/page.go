package storage

import (
	"encoding/binary"
	"unicode/utf8"
)

// Page is a object that holds contains of a disk block.
type Page interface {
	Int(offset int) int64
	SetInt(offset int, value int64)
	Bytes(offset int) []byte
	SetBytes(offset int, value []byte)
	String(offset int) string
	SetString(offset int, value string)
}

func NewPage(blocksize int) (Page, error) {
	return &localpage{data: make([]byte, blocksize)}, nil
}

type localpage struct {
	data []byte
}

func (p *localpage) Int(offset int) int64 {
	// Read bytes and convert to int64 - this handles both positive and negative values correctly
	// as the bit pattern is preserved in the conversion from uint64 to int64
	bytes := p.data[offset : offset+INT_SIZE]
	return int64(binary.LittleEndian.Uint64(bytes))
}

func (p *localpage) SetInt(offset int, value int64) {
	// Convert int64 to bytes using little-endian byte order
	// The bit pattern is preserved in the conversion from int64 to uint64,
	// so negative numbers will be correctly represented
	binary.LittleEndian.PutUint64(p.data[offset:offset+INT_SIZE], uint64(value))
}

func (p *localpage) Bytes(offset int) []byte {
	length := int(p.Int(offset))
	r := make([]byte, length)
	copy(r, p.data[offset+INT_SIZE:offset+INT_SIZE+length])
	return r
}

func (p *localpage) SetBytes(offset int, value []byte) {
	p.SetInt(offset, int64(len(value)))
	copy(p.data[offset+INT_SIZE:], value)
}

func (p *localpage) String(offset int) string {
	b := p.Bytes(offset)
	return string(b)
}

func (p *localpage) SetString(offset int, value string) {
	b := []byte(value)
	p.SetBytes(offset, b)
}

func MaxLength(length int) int {
	// Int64 bytes (for length) + maximum possible UTF-8 bytes
	return INT_SIZE + (length * utf8.UTFMax)
}

func (p *localpage) Contents() []byte {
	return p.data
}
