package recovery

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

var _ LogRecord = (*LogRecordRollback)(nil)

func NewLogRecordRollback(p *file.Page) *LogRecordRollback {
	tpos := file.INT_SIZE
	txnum := p.Int(tpos)

	return &LogRecordRollback{txnum: txnum}
}

type LogRecordRollback struct {
	txnum int
}

func (lr *LogRecordRollback) Op() int {
	return int(OpRollback)
}

func (lr *LogRecordRollback) TxNumber() int {
	return lr.txnum
}

func (lr *LogRecordRollback) Undo(tx transaction.Transaction) (err error) {
	return nil
}

func (lr *LogRecordRollback) String() string {
	return fmt.Sprintf("<Rollback %d>", lr.txnum)
}

func WriteRollbackLogRecord(lm log.LogManager, txnum int) (int, error) {
	rec := make([]byte, file.INT_SIZE*2)
	p := file.NewPageWithBuffer(rec)
	p.SetInt(0, int(OpRollback))
	p.SetInt(file.INT_SIZE, txnum)
	return lm.Append(rec)
}
