package metadata

import (
	"errors"

	"github.com/kanthorlabs/kanthorkv/index"
	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

func NewIndexMgr(isNew bool, tablemgr *TableMgr, statmgr *StatMgr, tx transaction.Transaction) (*IndexMgr, error) {
	if isNew {
		sche := record.NewSchema()
		sche.AddStringField("indexname", 16)
		sche.AddStringField("tablename", 16)
		sche.AddStringField("fieldname", 16)
		if err := tablemgr.CreateTable("idxcat", sche, tx); err != nil {
			return nil, err
		}
	}

	layout, err := tablemgr.GetLayout("idxcat", tx)
	if err != nil {
		return nil, err
	}

	return &IndexMgr{
		layout:   layout,
		tablemgr: tablemgr,
		statmgr:  statmgr,
	}, nil
}

type IndexMgr struct {
	layout   *record.Layout
	tablemgr *TableMgr
	statmgr  *StatMgr
}

func (im *IndexMgr) CreateIndex(idxname, tblname, fldname string, tx transaction.Transaction) error {
	ts, err := record.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return err
	}
	if err := ts.Insert(); err != nil {
		return err
	}

	if err := ts.SetString("indexname", idxname); err != nil {
		return err
	}
	if err := ts.SetString("tablename", tblname); err != nil {
		return err
	}
	if err := ts.SetString("fieldname", fldname); err != nil {
		return err
	}

	return ts.Close()

}

func (im *IndexMgr) GetIndexInfo(tblname string, tx transaction.Transaction) (results map[string]*IndexInfo, err error) {
	ts, err := record.NewTableScan(tx, "idxcat", im.layout)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, ts.Close())
	}()
	results = make(map[string]*IndexInfo)

	for ts.Next() {
		name, err := ts.GetString("tablename")
		if err != nil {
			return nil, err
		}
		if name == tblname {
			idxname, err := ts.GetString("indexname")
			if err != nil {
				return nil, err
			}
			fldname, err := ts.GetString("fieldname")
			if err != nil {
				return nil, err
			}
			tbllayout, err := im.tablemgr.GetLayout(tblname, tx)
			if err != nil {
				return nil, err
			}
			tblsi, err := im.statmgr.GetStatInfo(tblname, tbllayout, tx)
			if err != nil {
				return nil, err
			}

			results[fldname] = &IndexInfo{
				idxname:   idxname,
				fldname:   fldname,
				tx:        tx,
				tblSchema: tbllayout.Schema(),
				idxLayout: tbllayout,
				si:        tblsi,
			}
		}
	}

	return
}

type IndexInfo struct {
	idxname   string
	fldname   string
	tx        transaction.Transaction
	tblSchema *record.Schema
	idxLayout *record.Layout
	si        *StatInfo
}

func (ii *IndexInfo) Open() (index.Index, error) {
	return index.NewStaticHashIndex(ii.tx, ii.idxname, ii.idxLayout)
}

func (ii *IndexInfo) BlocksAccessed() int {
	rpb := ii.tx.BlockSize() / ii.idxLayout.SlotSize()
	numblocks := ii.si.RecordsOutput() / rpb
	return numblocks
}

func (ii *IndexInfo) RecordsOutput() int {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fldname)
}

func (ii *IndexInfo) DistinctValues(fldname string) int {
	if fldname == ii.fldname {
		return 1
	}
	return ii.si.DistinctValues(fldname)
}

func (ii *IndexInfo) CreateIdxLayout() *record.Layout {
	sche := record.NewSchema()
	sche.AddIntField("block")
	sche.AddIntField("id")
	if ii.tblSchema.Type(ii.fldname) == record.IntegerField {
		sche.AddIntField(ii.fldname)
	} else {
		fldlen := ii.tblSchema.Length(ii.fldname)
		sche.AddStringField(ii.fldname, fldlen)
	}
	return record.NewLayoutOfSchema(sche)
}
