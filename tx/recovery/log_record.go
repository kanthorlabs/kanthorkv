package recovery

import (
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/tx"
)

type LogOperation int

const (
	OpCheckpoint LogOperation = iota
	OpStart
	OpCommit
	OpRollback
	OpSetInt
	OpSetString
)

type LogRecord interface {
	Op() int
	TxNumber() int
	Undo(tx tx.Transaction) error
}

func NewLogRecord(bytes []byte) (LogRecord, error) {
	p := file.NewPageWithBuffer(bytes)
	op := p.Int(0)
	switch LogOperation(op) {
	case OpCheckpoint:
		return NewLogRecordCheckpoint(), nil
	case OpStart:
		return NewLogRecordStart(p), nil
	case OpCommit:
		return NewLogRecordCommit(p), nil
	case OpRollback:
		return NewLogRecordRollback(p), nil
	case OpSetInt:
		return NewLogRecordSetInt(p), nil
	case OpSetString:
		return NewLogRecordSetString(p), nil
	default:
		return nil, ErrInvalidLogRecord(op)
	}
}
