package recovery

import (
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/kanthorlabs/kanthorkv/tx"
)

var _ LogRecord = (*LogRecordCheckpoint)(nil)

func NewLogRecordCheckpoint() *LogRecordCheckpoint {
	return &LogRecordCheckpoint{}
}

type LogRecordCheckpoint struct{}

func (lr *LogRecordCheckpoint) Op() int {
	return int(OpCheckpoint)
}

func (lr *LogRecordCheckpoint) TxNumber() int {
	return -1 // dummy value, as checkpoints are not associated with a specific transaction
}

func (lr *LogRecordCheckpoint) Undo(tx tx.Transaction) (err error) {
	return nil
}

func (lr *LogRecordCheckpoint) String() string {
	return "<CHECKPOINT>"
}

func WriteCheckpointLogRecord(lm log.LogManager, txnum int) (int, error) {
	rec := make([]byte, file.INT_SIZE*2)
	p := file.NewPageWithBuffer(rec)
	p.SetInt(0, int(OpCheckpoint))
	p.SetInt(file.INT_SIZE, txnum)
	return lm.Append(rec)
}
