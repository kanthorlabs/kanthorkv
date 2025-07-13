package recovery

import (
	"errors"
	"fmt"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/kanthorlabs/kanthorkv/tx"
)

var _ LogRecord = (*LogRecordSetInt)(nil)

func NewLogRecordSetInt(p *file.Page) *LogRecordSetInt {
	tpos := file.INT_SIZE
	txnum := p.Int(tpos)

	fpos := tpos + file.INT_SIZE
	filename := p.String(fpos)

	bpos := fpos + file.MaxLength(len(filename))
	blknum := p.Int(bpos)
	blk := file.NewBlockId(filename, blknum)

	opos := bpos + file.INT_SIZE
	offset := p.Int(opos)

	vpos := opos + file.INT_SIZE
	val := p.Int(vpos)

	return &LogRecordSetInt{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

type LogRecordSetInt struct {
	txnum  int
	offset int
	val    int
	blk    *file.BlockId
}

func (lr *LogRecordSetInt) Op() int {
	return int(OpSetInt)
}

func (lr *LogRecordSetInt) TxNumber() int {
	return lr.txnum
}

func (lr *LogRecordSetInt) Undo(tx tx.Transaction) (err error) {
	if err := tx.Pin(lr.blk); err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, tx.Unpin(lr.blk))
	}()
	err = tx.SetInt(lr.blk, lr.offset, lr.val, false) // don't log the undo!
	return
}

func (lr *LogRecordSetInt) String() string {
	return fmt.Sprintf("<SETINT val=%d blk=%s offset=%d txnum=%d>", lr.val, lr.blk.String(), lr.offset, lr.txnum)
}

func WriteSetIntLogRecord(lm log.LogManager, txnum int, blk *file.BlockId, offset int, val int) (int, error) {
	tpos := file.INT_SIZE
	fpos := tpos + file.INT_SIZE
	bpos := fpos + file.MaxLength(len(blk.Filename()))
	opos := bpos + file.INT_SIZE
	vpos := opos + file.INT_SIZE
	reclen := vpos + file.INT_SIZE
	rec := make([]byte, reclen)
	p := file.NewPageWithBuffer(rec)
	p.SetInt(0, int(OpSetInt))
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.Filename())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)
	return lm.Append(rec)
}
