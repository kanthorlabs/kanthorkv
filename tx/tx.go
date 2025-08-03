package tx

import (
	"errors"
	"sync/atomic"

	"github.com/kanthorlabs/kanthorkv/buffer"
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/kanthorlabs/kanthorkv/tx/concurrency"
	"github.com/kanthorlabs/kanthorkv/tx/recovery"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

var _ transaction.Transaction = (*txn)(nil)

func NewTransaction(fm file.FileManager, lm log.LogManager, bm buffer.BufferManager, lt *concurrency.LockTable) (transaction.Transaction, error) {
	tx := txn{
		fm:    fm,
		bm:    bm,
		cm:    concurrency.NewConcurrencyManager(lt),
		txnum: int(nextTxNum.Add(1)),
		bl:    NewBufferList(bm),
	}
	tx.rm = recovery.NewRecoveryManager(lm, bm, &tx, tx.txnum)
	return &tx, nil
}

var (
	nextTxNum atomic.Uint32
	endofFile = -1
)

type txn struct {
	fm    file.FileManager
	bm    buffer.BufferManager
	rm    recovery.RecoveryManager
	cm    *concurrency.ConcurrencyManager
	txnum int
	bl    *BufferList
}

// transaction’s lifespan

func (tx *txn) Commit() error {
	if err := tx.rm.Commit(); err != nil {
		return err
	}
	defer tx.cm.Release()
	defer tx.bl.UnpinAll()
	return nil
}

func (tx *txn) Rollback() error {
	if err := tx.rm.Rollback(); err != nil {
		return err
	}
	defer tx.cm.Release()
	defer tx.bl.UnpinAll()
	return nil
}

func (tx *txn) Recover() error {
	if err := tx.bm.FlushAll(tx.txnum); err != nil {
		return err
	}

	if err := tx.rm.Recover(); err != nil {
		return err
	}
	return nil
}

// buffer manager

func (tx *txn) Pin(blk *file.BlockId) error {
	return tx.bl.Pin(blk)
}

func (tx *txn) Unpin(blk *file.BlockId) error {
	return tx.bl.Unpin(blk)
}

func (tx *txn) GetInt(blk *file.BlockId, offset int) (int, error) {
	if err := tx.cm.SLock(blk); err != nil {
		return 0, err
	}
	buff, ok := tx.bl.Get(blk)
	if !ok {
		return 0, errors.New("buffer of block is not found, pin it first")
	}
	return buff.Contents.Int(offset), nil
}

func (tx *txn) GetString(blk *file.BlockId, offset int) (string, error) {
	if err := tx.cm.SLock(blk); err != nil {
		return "", err
	}
	buff, ok := tx.bl.Get(blk)
	if !ok {
		return "", errors.New("buffer of block is not found, pin it first")
	}
	return buff.Contents.String(offset), nil
}

func (tx *txn) SetInt(blk *file.BlockId, offset int, val int, shouldLog bool) error {
	if err := tx.cm.XLock(blk); err != nil {
		return err
	}
	b, ok := tx.bl.Get(blk)
	if !ok {
		return errors.New("buffer of block is not found, pin it first")
	}

	var err error
	lsn := -1
	if shouldLog {
		lsn, err = tx.rm.SetInt(b, offset, val)
		if err != nil {
			return err
		}
	}

	p := b.Contents
	p.SetInt(offset, val)
	b.SetModified(tx.txnum, lsn)
	return nil
}

func (tx *txn) SetString(blk *file.BlockId, offset int, val string, shouldLog bool) error {
	if err := tx.cm.XLock(blk); err != nil {
		return err
	}
	b, ok := tx.bl.Get(blk)
	if !ok {
		return errors.New("buffer of block is not found, pin it first")
	}

	var err error
	lsn := -1
	if shouldLog {
		lsn, err = tx.rm.SetString(b, offset, val)
		if err != nil {
			return err
		}
	}

	p := b.Contents
	p.SetString(offset, val)
	b.SetModified(tx.txnum, lsn)
	return nil
}

func (tx *txn) AvailableBuffs() int {
	return tx.bm.Available()
}

// file manager

func (tx *txn) Size(filename string) (int, error) {
	dummy := file.NewBlockId(filename, endofFile)
	if err := tx.cm.SLock(dummy); err != nil {
		return 0, err
	}
	return tx.fm.Length(filename)
}

func (tx *txn) Append(filename string) (*file.BlockId, error) {
	dummy := file.NewBlockId(filename, endofFile)
	if err := tx.cm.XLock(dummy); err != nil {
		return nil, err
	}
	return tx.fm.Append(filename)
}

func (tx *txn) BlockSize() int {
	return tx.fm.BlockSize()
}
