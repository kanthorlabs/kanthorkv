package tx

import (
	"github.com/kanthorlabs/kanthorkv/buffer"
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
)

func NewTransaction(fm file.FileManager, lm log.LogManager, bm buffer.BufferManager) (Transaction, error) {
	return nil, nil
}

type Transaction interface {
	// transaction’s lifespan
	Commit() error
	Rollback() error
	Recover() error

	// buffer manager
	Pin(blk *file.BlockId) error
	Unpin(blk *file.BlockId) error
	GetInt(blk *file.BlockId, offset int) (int, error)
	GetString(blk *file.BlockId, offset int) (string, error)
	SetInt(blk *file.BlockId, offset int, val int, shouldLog bool) error
	SetString(blk *file.BlockId, offset int, val string, shouldLog bool) error
	AvailableBuffs() int

	// file manager
	Size(filename string) (int, error)
	Append(filename string) (*file.BlockId, error)
	BlockSize() int
}
