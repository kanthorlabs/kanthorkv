package tx

import (
	"errors"

	"fmt"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
)

var _ LogRecord = (*LogRecordSetString)(nil)

func NewLogRecordSetString(p *file.Page) *LogRecordSetString {
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
	val := p.String(vpos)

	return &LogRecordSetString{
		txnum:  txnum,
		offset: offset,
		val:    val,
		blk:    blk,
	}
}

type LogRecordSetString struct {
	txnum  int
	offset int
	val    string
	blk    *file.BlockId
}

func (lr *LogRecordSetString) Op() int {
	return int(OpSetString)
}

func (lr *LogRecordSetString) TxNumber() int {
	return lr.txnum
}

func (lr *LogRecordSetString) Undo(tx Transaction) (err error) {
	if err := tx.Pin(lr.blk); err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, tx.Unpin(lr.blk))
	}()
	err = tx.SetString(lr.blk, lr.offset, lr.val, false) // don't log the undo!
	return
}

func (lr *LogRecordSetString) String() string {
	return fmt.Sprintf("<SETSTRING val=%q blk=%s offset=%d txnum=%d>", lr.val, lr.blk.String(), lr.offset, lr.txnum)
}

func WriteSetStringLogRecord(lm log.LogManager, txnum int, blk *file.BlockId, offset int, val string) (int, error) {
	tpos := file.INT_SIZE
	fpos := tpos + file.INT_SIZE
	bpos := fpos + file.MaxLength(len(blk.Filename()))
	opos := bpos + file.INT_SIZE
	vpos := opos + file.INT_SIZE
	reclen := vpos + file.MaxLength(len(val))
	rec := make([]byte, reclen)
	p := file.NewPageWithBuffer(rec)
	p.SetInt(0, int(OpSetString))
	p.SetInt(tpos, txnum)
	p.SetString(fpos, blk.Filename())
	p.SetInt(bpos, blk.Number())
	p.SetInt(opos, offset)
	p.SetString(vpos, val)
	return lm.Append(rec)
}
