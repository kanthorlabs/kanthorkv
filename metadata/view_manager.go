package metadata

import (
	"errors"

	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

// view name max length
const VIEW_MAX_LEN = 100

func NewViewMgr(isNew bool, tblmgr *TableMgr, tx transaction.Transaction) (*ViewMgr, error) {
	vmgr := &ViewMgr{
		tblmgr: tblmgr,
	}
	if isNew {
		sch := record.NewSchema()
		sch.AddStringField("viewname", TABLE_MAX_LEN)
		sch.AddStringField("viewdef", VIEW_MAX_LEN)

		if err := vmgr.tblmgr.CreateTable("viewcat", sch, tx); err != nil {
			return nil, err
		}
	}

	return vmgr, nil
}

type ViewMgr struct {
	tblmgr *TableMgr
}

func (vm *ViewMgr) CreateView(vname string, vdef string, tx transaction.Transaction) error {
	layout, err := vm.tblmgr.GetLayout("viewcat", tx)
	if err != nil {
		return err
	}

	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, ts.Close())
	}()

	if err := ts.Insert(); err != nil {
		return err
	}
	if err := ts.SetString("viewname", vname); err != nil {
		return err
	}
	if err := ts.SetString("viewdef", vdef); err != nil {
		return err
	}

	return nil
}

func (vm *ViewMgr) GetViewDef(vname string, tx transaction.Transaction) (string, error) {
	layout, err := vm.tblmgr.GetLayout("viewcat", tx)
	if err != nil {
		return "", err
	}

	ts, err := record.NewTableScan(tx, "viewcat", layout)
	if err != nil {
		return "", err
	}
	defer func() {
		err = errors.Join(err, ts.Close())
	}()

	for ts.Next() {
		viewname, err := ts.GetString("viewname")
		if err != nil {
			return "", err
		}
		if viewname == vname {
			return ts.GetString("viewdef")
		}
	}

	return "", nil
}
