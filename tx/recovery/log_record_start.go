package recovery

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/kanthorlabs/kanthorkv/tx"
)

var _ LogRecord = (*LogRecordStart)(nil)

func NewLogRecordStart(p *file.Page) *LogRecordStart {
	tpos := file.INT_SIZE
	txnum := p.Int(tpos)

	return &LogRecordStart{txnum: txnum}
}

type LogRecordStart struct {
	txnum int
}

func (lr *LogRecordStart) Op() int {
	return int(OpStart)
}

func (lr *LogRecordStart) TxNumber() int {
	return lr.txnum
}

func (lr *LogRecordStart) Undo(tx tx.Transaction) (err error) {
	return nil
}

func (lr *LogRecordStart) String() string {
	return fmt.Sprintf("<START %d>", lr.txnum)
}

func WriteStartLogRecord(lm log.LogManager) (int, error) {
	rec := make([]byte, file.INT_SIZE)
	p := file.NewPageWithBuffer(rec)
	p.SetInt(0, int(OpStart))
	return lm.Append(rec)
}
