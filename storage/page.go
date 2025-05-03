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

func NewPageWithBuffer(buffer []byte) (*Page, error) {
	if len(buffer) <= 0 {
		return nil, ErrPageInvalidBlockSize(len(buffer))
	}
	return &Page{buffer: buffer}, nil
}

// Page is a object that holds contains of a disk block on memory.
type Page struct {
	buffer []byte
}

func (p *Page) Int(offset int) int {
	// Read bytes and convert to int - this handles both positive and negative values correctly.
	// as the bit pattern is preserved in the conversion from uint to int.
	bytes := p.buffer[offset : INT_SIZE+offset]
	return int(binary.LittleEndian.Uint32(bytes))
}

func (p *Page) SetInt(offset int, value int) error {
	// Check if there's enough space in the buffer
	if offset < 0 || INT_SIZE+offset > len(p.buffer) {
		return ErrPageSetIntBufferOverflow(offset, INT_SIZE, len(p.buffer))
	}

	// Convert int to bytes using little-endian byte order
	// The bit pattern is preserved in the conversion from int to uint,
	// so negative numbers will be correctly represented
	binary.LittleEndian.PutUint32(p.buffer[offset:INT_SIZE+offset], uint32(value))
	return nil
}

func (p *Page) Bytes(offset int) []byte {
	length := int(p.Int(offset))
	r := make([]byte, length)
	copy(r, p.buffer[INT_SIZE+offset:INT_SIZE+offset+length])
	return r
}

func (p *Page) SetBytes(offset int, value []byte) error {
	if INT_SIZE+offset+len(value) > len(p.buffer) {
		return ErrPageSetBytesBufferOverflow(offset, len(value), len(p.buffer))
	}

	if err := p.SetInt(offset, int(len(value))); err != nil {
		return err
	}
	copy(p.buffer[INT_SIZE+offset:], value)
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
