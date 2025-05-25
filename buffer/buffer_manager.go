package buffer

import (
	"sync"
	"time"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
)

const MAX_TIME = 10 * time.Second

type BufferManager interface {
	Available() int
	FlushAll(txnum int) error
	Unpin(buf *Buffer)
	Pin(blk *file.BlockId) (*Buffer, error)
}

var _ BufferManager = (*localbm)(nil)

func NewBufferManager(fm file.FileManager, lm log.LogManager, numbuffs int) (BufferManager, error) {
	bm := &localbm{
		bufferpool:   make([]*Buffer, numbuffs),
		numavailable: numbuffs,
		maxtime:      MAX_TIME,
		mu:           &sync.Mutex{},
		waiters:      make(map[int]chan bool),
	}

	for i := range numbuffs {
		buf, err := NewBuffer(fm, lm)
		if err != nil {
			return nil, err
		}
		bm.bufferpool[i] = buf
	}

	return bm, nil
}

type localbm struct {
	bufferpool   []*Buffer
	numavailable int
	maxtime      time.Duration

	mu      *sync.Mutex
	waiters map[int]chan bool
}

func (bm *localbm) Available() int {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	return bm.numavailable
}

func (bm *localbm) FlushAll(txnum int) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	for _, buf := range bm.bufferpool {
		if buf.ModifyingTx == txnum {
			if err := buf.Flush(); err != nil {
				return err
			}
		}
	}

	return nil
}

func (bm *localbm) Unpin(buf *Buffer) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	buf.Unpin()
	if !buf.IsPinned() {
		bm.numavailable++
		// Signal all goroutines waiting for this file (and remove the channel)
		if ch, exists := bm.waiters[buf.Block.HashCode()]; exists {
			close(ch)
			delete(bm.waiters, buf.Block.HashCode())
		}
	}
}

func (bm *localbm) Pin(blk *file.BlockId) (*Buffer, error) {
	bm.mu.Lock()

	start := time.Now()
	buff, err := bm.tryPin(blk)
	if err != nil {
		bm.mu.Unlock()
		return nil, err
	}

	// If the buffer is not found or is already pinned, we need to wait
	for buff == nil {
		ch := bm.acquireChannel(blk)
		// if we don't unlock here, other goroutines will not be able to call .Pin()
		// so that we need to release the lock before waiting on the channel
		bm.mu.Unlock()

		if time.Since(start) > bm.maxtime {
			return nil, ErrBMPinTimeout(blk.String())
		}

		// Wait on the channel with a timeout
		select {
		case <-ch:
			// Continue when the lock is released
			bm.mu.Lock()
			buff, err = bm.tryPin(blk)
			if err != nil {
				bm.mu.Unlock()
				return nil, err
			}
		case <-time.After(bm.maxtime):
			return nil, ErrBMPinTimeout(blk.String())
		}
	}

	bm.mu.Unlock()
	return buff, nil
}

func (bm *localbm) tryPin(blk *file.BlockId) (*Buffer, error) {
	buff := bm.findExistingBuffer(blk)
	if buff == nil {
		buff = bm.chooseUnpinnedBuffer()
		if buff == nil {
			return nil, nil
		}

		if err := buff.AssignToBlock(blk); err != nil {
			return nil, err
		}
	}

	if !buff.IsPinned() {
		// if it's not pinned, we are the first to pin it,
		// so there's one less buffer available now
		bm.numavailable--
	}
	buff.Pin()
	return buff, nil
}

func (bm *localbm) findExistingBuffer(blk *file.BlockId) *Buffer {
	for _, buf := range bm.bufferpool {
		if buf.Block != nil && buf.Block.Equals(blk) {
			return buf
		}
	}
	return nil
}

// The Naïve Strategy
func (bm *localbm) chooseUnpinnedBuffer() *Buffer {
	for _, buf := range bm.bufferpool {
		if !buf.IsPinned() {
			return buf
		}
	}
	return nil
}

func (bm *localbm) acquireChannel(blk *file.BlockId) chan bool {
	if ch, exists := bm.waiters[blk.HashCode()]; exists {
		return ch
	}
	ch := make(chan bool)
	bm.waiters[blk.HashCode()] = ch
	return ch
}
