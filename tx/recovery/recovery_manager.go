package recovery

import (
	"github.com/kanthorlabs/kanthorkv/buffer"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

var _ RecoveryManager = (*localrm)(nil)

// RecoveryManager defines the interface for transaction recovery operations
type RecoveryManager interface {
	Commit() error
	Rollback() error
	Recover() error
	SetInt(buff *buffer.Buffer, offset int, newval int) (int, error)
	SetString(buff *buffer.Buffer, offset int, newval string) (int, error)
}

func NewRecoveryManager(lm log.LogManager, bm buffer.BufferManager, tx transaction.Transaction, txnum int) RecoveryManager {
	return &localrm{lm: lm, bm: bm, tx: tx, txnum: txnum}
}

type localrm struct {
	lm    log.LogManager
	bm    buffer.BufferManager
	tx    transaction.Transaction
	txnum int
}

func (rm *localrm) Commit() error {
	if err := rm.bm.FlushAll(rm.txnum); err != nil {
		return err
	}
	lsn, err := WriteCommitLogRecord(rm.lm, rm.txnum)
	if err != nil {
		return err
	}
	return rm.lm.Flush(lsn)
}

func (rm *localrm) Rollback() error {
	if err := rm.rollback(); err != nil {
		return err
	}
	lsn, err := WriteRollbackLogRecord(rm.lm, rm.txnum)
	if err != nil {
		return err
	}
	return rm.lm.Flush(lsn)
}

func (rm *localrm) rollback() error {
	iter, err := rm.lm.Iterator()
	if err != nil {
		return err
	}
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return err
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return err
		}
		if rec.TxNumber() == rm.txnum {
			if rec.Op() == int(OpStart) {
				return nil
			}
			if err := rec.Undo(rm.tx); err != nil {
				return err
			}
		}
	}

	return nil
}

func (rm *localrm) Recover() error {
	if err := rm.recover(); err != nil {
		return err
	}
	lsn, err := WriteCheckpointLogRecord(rm.lm, rm.txnum)
	if err != nil {
		return err
	}
	return rm.lm.Flush(lsn)
}

func (rm *localrm) recover() error {
	iter, err := rm.lm.Iterator()
	if err != nil {
		return err
	}

	finished := make(map[int]bool, 0)
	for iter.HasNext() {
		bytes, err := iter.Next()
		if err != nil {
			return err
		}
		rec, err := NewLogRecord(bytes)
		if err != nil {
			return err
		}
		// Checkpoint found, recovery complete
		if rec.Op() == int(OpCheckpoint) {
			return nil
		}
		// If the transaction is already finished, skip it
		if rec.Op() == int(OpCommit) || rec.Op() == int(OpRollback) {
			finished[rec.TxNumber()] = true
			continue
		}
		// If the transaction is not finished, we need to undo it
		if exist, ok := finished[rec.TxNumber()]; !exist || !ok {
			if err := rec.Undo(rm.tx); err != nil {
				return err
			}
		}
	}

	return nil
}

// newval isn't used because the recovery algorithm is undo-only
func (rm *localrm) SetInt(buff *buffer.Buffer, offset int, newval int) (int, error) {
	oldval := buff.Contents.Int(offset)
	return WriteSetIntLogRecord(rm.lm, rm.txnum, buff.Block, offset, oldval)
}

// newval isn't used because the recovery algorithm is undo-only
func (rm *localrm) SetString(buff *buffer.Buffer, offset int, newval string) (int, error) {
	oldval := buff.Contents.String(offset)
	return WriteSetStringLogRecord(rm.lm, rm.txnum, buff.Block, offset, oldval)
}
