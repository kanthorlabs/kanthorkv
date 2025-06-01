package tx

import (
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
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

func (lr *LogRecordCheckpoint) Undo(tx Transaction) (err error) {
	return nil
}

func (lr *LogRecordCheckpoint) String() string {
	return "<CHECKPOINT>"
}

func WriteCheckpointLogRecord(lm log.LogManager) (int, error) {
	rec := make([]byte, file.INT_SIZE)
	p := file.NewPageWithBuffer(rec)
	p.SetInt(0, int(OpCheckpoint))
	return lm.Append(rec)
}
