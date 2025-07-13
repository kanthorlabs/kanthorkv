package recovery

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/kanthorlabs/kanthorkv/tx"
)

var _ LogRecord = (*LogRecordCommit)(nil)

func NewLogRecordCommit(p *file.Page) *LogRecordCommit {
	tpos := file.INT_SIZE
	txnum := p.Int(tpos)

	return &LogRecordCommit{txnum: txnum}
}

type LogRecordCommit struct {
	txnum int
}

func (lr *LogRecordCommit) Op() int {
	return int(OpCommit)
}

func (lr *LogRecordCommit) TxNumber() int {
	return lr.txnum
}

func (lr *LogRecordCommit) Undo(tx tx.Transaction) (err error) {
	return nil
}

func (lr *LogRecordCommit) String() string {
	return fmt.Sprintf("<COMMIT %d>", lr.txnum)
}

func WriteCommitLogRecord(lm log.LogManager, txnum int) (int, error) {
	rec := make([]byte, file.INT_SIZE*2)
	p := file.NewPageWithBuffer(rec)
	p.SetInt(0, int(OpCommit))
	p.SetInt(file.INT_SIZE, txnum)
	return lm.Append(rec)
}
