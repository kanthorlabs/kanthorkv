package metadata

import (
	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

func NewMetadataMgr(isNew bool, tx transaction.Transaction) (*MetadataMgr, error) {
	tablemgr, err := NewTableMgr(isNew, tx)
	if err != nil {
		return nil, err
	}
	viewmgr, err := NewViewMgr(isNew, tablemgr, tx)
	if err != nil {
		return nil, err
	}
	statmgr, err := NewStatMgr(tablemgr, tx)
	if err != nil {
		return nil, err
	}
	indexmgr, err := NewIndexMgr(isNew, tablemgr, statmgr, tx)
	if err != nil {
		return nil, err
	}
	return &MetadataMgr{
		tablemgr: tablemgr,
		viewmgr:  viewmgr,
		statmgr:  statmgr,
		indexmgr: indexmgr,
	}, nil
}

type MetadataMgr struct {
	tablemgr *TableMgr
	viewmgr  *ViewMgr
	statmgr  *StatMgr
	indexmgr *IndexMgr
}

func (mm *MetadataMgr) CreateTable(tblname string, sche *record.Schema, tx transaction.Transaction) error {
	return mm.tablemgr.CreateTable(tblname, sche, tx)
}

func (mm *MetadataMgr) GetLayout(tblname string, tx transaction.Transaction) (*record.Layout, error) {
	return mm.tablemgr.GetLayout(tblname, tx)
}

func (mm *MetadataMgr) CreateView(viewname, viewdef string, tx transaction.Transaction) error {
	return mm.viewmgr.CreateView(viewname, viewdef, tx)
}

func (mm *MetadataMgr) GetViewDef(viewname string, tx transaction.Transaction) (string, error) {
	return mm.viewmgr.GetViewDef(viewname, tx)
}

func (mm *MetadataMgr) CreateIndex(idxname, tblname, fldname string, tx transaction.Transaction) error {
	return mm.indexmgr.CreateIndex(idxname, tblname, fldname, tx)
}

func (mm *MetadataMgr) GetIndexInfo(tblname string, tx transaction.Transaction) (map[string]*IndexInfo, error) {
	return mm.indexmgr.GetIndexInfo(tblname, tx)
}

func (mm *MetadataMgr) GetStatInfo(tblname string, layout *record.Layout, tx transaction.Transaction) (*StatInfo, error) {
	return mm.statmgr.GetStatInfo(tblname, layout, tx)
}
