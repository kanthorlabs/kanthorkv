package buffer

import (
	"sync/atomic"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
)

var counter atomic.Uint32

func NewBuffer(fm file.FileManager, lm log.LogManager) (*Buffer, error) {
	p, err := file.NewPage(fm.BlockSize())
	if err != nil {
		return nil, err
	}

	buf := &Buffer{
		Contents:    p,
		Block:       nil,
		ModifyingTx: -1,
		fm:          fm,
		lm:          lm,
		pins:        0,
		lsn:         -1,
		// a hack to start the id from 0
		id: int(counter.Add(1) - 1),
	}
	return buf, nil
}

type Buffer struct {
	Contents    *file.Page
	Block       *file.BlockId
	ModifyingTx int
	fm          file.FileManager
	lm          log.LogManager
	pins        int
	lsn         int
	id          int
}

func (b *Buffer) SetModified(txnum int, lsn int) {
	b.ModifyingTx = txnum
	if lsn > 0 {
		b.lsn = lsn
	}
}

func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
}

func (b *Buffer) Flush() error {
	if b.ModifyingTx >= 0 {
		if err := b.lm.Flush(b.lsn); err != nil {
			return err
		}
		if err := b.fm.Write(b.Block, b.Contents); err != nil {
			return err
		}
		b.ModifyingTx = -1
	}

	return nil
}

func (b *Buffer) AssignToBlock(blk *file.BlockId) error {
	if err := b.Flush(); err != nil {
		return err
	}
	if err := b.fm.Read(blk, b.Contents); err != nil {
		return err
	}

	b.Block = blk
	b.pins = 0
	return nil
}
