package buffer

import (
	"sync"
	"time"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
)

const MAX_TIME = 10

type BufferManager interface {
	Available() int
	FlushAll(txnum int) error
	Unpin(buf *Buffer) error
	Pin(blk *file.BlockId) (*Buffer, error)
}

var _ BufferManager = (*localbm)(nil)

func NewBufferManager(fm file.FileManager, lm log.LogManager, numbuffs int) (BufferManager, error) {
	bm := &localbm{
		bufferpool:   make([]*Buffer, numbuffs),
		numavailable: numbuffs,
		maxtime:      MAX_TIME,
		mu:           &sync.Mutex{},
	}

	for i := 0; i < numbuffs; i++ {
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
	maxtime      int64
	mu           *sync.Mutex
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

func (bm *localbm) Unpin(buf *Buffer) error {
	if buf == nil {
		return nil
	}

	bm.mu.Lock()
	defer bm.mu.Unlock()

	buf.Unpin()
	if !buf.IsPinned() {
		bm.numavailable++

		// TODO: notifyAll();
	}
}

func (bm *localbm) Pin(blk *file.BlockId) (*Buffer, error) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	now := time.Now().UnixMilli()
	buff, err := bm.tryPin(blk)
	if err != nil {
		return nil, err
	}

	for buff == nil && !bm.waitingTooLong(now) {
		buff, err = bm.tryPin(blk)
	}
}

func (bm *localbm) tryPin(blk *file.BlockId) (*Buffer, error) {}

func (bm *localbm) waitingTooLong(start int64) bool {
	if time.Now().UnixMilli()-start > bm.maxtime {
		return true
	}
	return false
}
