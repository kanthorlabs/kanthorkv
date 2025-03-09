package storage

import (
	"encoding/binary"
)

func NewPage(blksize int) (*Page, error) {
	if blksize <= 0 {
		return nil, ErrPageInvalidBlockSize(blksize)
	}
	return &Page{buffer: make([]byte, blksize)}, nil
}

// Page is a object that holds contains of a disk block on memory.
type Page struct {
	buffer []byte
}

func (p *Page) Int(offset int) int64 {
	// Read bytes and convert to int64 - this handles both positive and negative values correctly.
	// as the bit pattern is preserved in the conversion from uint64 to int64.
	bytes := p.buffer[offset : offset+INT64_SIZE]
	return int64(binary.LittleEndian.Uint64(bytes))
}

func (p *Page) SetInt(offset int, value int64) error {
	// Check if there's enough space in the buffer
	if offset < 0 || offset+INT64_SIZE > len(p.buffer) {
		return ErrPageSetIntBufferOverflow(offset, INT64_SIZE, len(p.buffer))
	}

	// Convert int64 to bytes using little-endian byte order
	// The bit pattern is preserved in the conversion from int64 to uint64,
	// so negative numbers will be correctly represented
	binary.LittleEndian.PutUint64(p.buffer[offset:offset+INT64_SIZE], uint64(value))
	return nil
}

func (p *Page) Bytes(offset int) []byte {
	length := int(p.Int(offset))
	r := make([]byte, length)
	copy(r, p.buffer[offset+INT64_SIZE:offset+INT64_SIZE+length])
	return r
}

func (p *Page) SetBytes(offset int, value []byte) error {
	if offset+INT64_SIZE+len(value) > len(p.buffer) {
		return ErrPageSetBytesBufferOverflow(offset, len(value), len(p.buffer))
	}

	if err := p.SetInt(offset, int64(len(value))); err != nil {
		return err
	}
	copy(p.buffer[offset+INT64_SIZE:], value)
	return nil
}

func (p *Page) String(offset int) string {
	b := p.Bytes(offset)
	return string(b)
}

func (p *Page) SetString(offset int, value string) error {
	b := []byte(value)
	return p.SetBytes(offset, b)
}
