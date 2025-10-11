package plan

import (
	"github.com/kanthorlabs/kanthorkv/metadata"
	"github.com/kanthorlabs/kanthorkv/query"
	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

var _ query.Plan = (*TablePlan)(nil)

func NewTablePlan(tblname string, tx transaction.Transaction, mdm *metadata.MetadataMgr) (*TablePlan, error) {
	tp := &TablePlan{
		tblname: tblname,
		tx:      tx,
	}

	layout, err := mdm.GetLayout(tblname, tx)
	if err != nil {
		return nil, err
	}
	tp.layout = layout

	si, err := mdm.GetStatInfo(tblname, layout, tx)
	if err != nil {
		return nil, err
	}
	tp.si = si

	return tp, nil
}

type TablePlan struct {
	tblname string
	tx      transaction.Transaction
	layout  *record.Layout
	si      *metadata.StatInfo
}

func (tp *TablePlan) Open() (record.Scan, error) {
	return record.NewTableScan(tp.tx, tp.tblname, tp.layout)
}

func (tp *TablePlan) BlocksAccessed() int {
	return tp.si.BlocksAccessed()
}

func (tp *TablePlan) RecordsOutput() int {
	return tp.si.RecordsOutput()
}

func (tp *TablePlan) DistinctValues(fldname string) int {
	return tp.si.DistinctValues(fldname)
}

func (tp *TablePlan) Schema() *record.Schema {
	return tp.layout.Schema()
}
