package tx

import (
	"github.com/kanthorlabs/kanthorkv/buffer"
	"github.com/kanthorlabs/kanthorkv/file"
)

func NewBufferList(bm buffer.BufferManager) *BufferList {
	return &BufferList{
		buffers: make(map[*file.BlockId]*buffer.Buffer),
		pins:    make(map[*file.BlockId]int),
		bm:      bm,
	}
}

type BufferList struct {
	buffers map[*file.BlockId]*buffer.Buffer
	// keep track of how many times each block has been pinned
	pins map[*file.BlockId]int
	bm   buffer.BufferManager
}

func (bl *BufferList) Get(blk *file.BlockId) (*buffer.Buffer, bool) {
	buf, exists := bl.buffers[blk]
	return buf, exists
}

func (bl *BufferList) Pin(blk *file.BlockId) error {
	b, err := bl.bm.Pin(blk)
	if err != nil {
		return err
	}

	bl.buffers[blk] = b
	bl.pins[blk]++
	return nil
}

func (bl *BufferList) Unpin(blk *file.BlockId) error {
	b, exists := bl.buffers[blk]
	if !exists {
		return nil
	}
	bl.bm.Unpin(b)
	delete(bl.pins, blk)
	// remove pins
	delete(bl.buffers, blk)
	return nil
}

func (bl *BufferList) UnpinAll() {
	for blk := range bl.pins {
		b := bl.buffers[blk]
		bl.bm.Unpin(b)
	}

	bl.buffers = make(map[*file.BlockId]*buffer.Buffer)
	bl.pins = make(map[*file.BlockId]int)
}
