package query

import (
	"fmt"
	"sync"

	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

func NewTempTable(tx transaction.Transaction, sch *record.Schema) *TempTable {
	layout := record.NewLayoutOfSchema(sch)

	return &TempTable{
		tx:        tx,
		TableName: newTableName(),
		layout:    layout,
	}
}

type TempTable struct {
	tx          transaction.Transaction
	layout      *record.Layout
	TableName   string
	TotalBlkNum int
}

func (tt *TempTable) Open() (*record.TableScan, error) {
	scan, err := record.NewTableScan(tt.tx, tt.TableName, tt.layout)
	if err != nil {
		return nil, fmt.Errorf("tt.Open: %w", err)
	}
	return scan, nil
}

func (tt *TempTable) Layout() *record.Layout {
	return tt.layout
}

var nextTableNum = 0
var mux = &sync.Mutex{}

func newTableName() string {
	mux.Lock()
	defer mux.Unlock()

	nextTableNum++
	return fmt.Sprintf("temp_%d", nextTableNum)
}
