package tx

import (
	"errors"

	"github.com/kanthorlabs/kanthorkv/buffer"
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

func NewTransaction(fm file.FileManager, lm log.LogManager, bm buffer.BufferManager) (transaction.Transaction, error) {
	return nil, errors.New("transaction creation not implemented")
}
