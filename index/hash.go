package index

import (
	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

var _ Index = (*HashIndex)(nil)

func NewHashIndex(tx transaction.Transaction, idxname string, idxLayout *record.Layout) (Index, error) {
	return &HashIndex{
		tx:        tx,
		idxname:   idxname,
		idxLayout: idxLayout,
	}, nil
}

type HashIndex struct {
	tx        transaction.Transaction
	idxname   string
	idxLayout *record.Layout
}

func (hi *HashIndex) SearchCost(numblocks, rpb int) int {
	return 3 + (numblocks / (2 * rpb))
}
