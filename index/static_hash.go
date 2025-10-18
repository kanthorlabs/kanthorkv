package index

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

var _ Index = (*StaticHashIndex)(nil)

func NewStaticHashIndex(tx transaction.Transaction, idxName string, idxLayout *record.Layout) (Index, error) {
	return &StaticHashIndex{
		NumBuckets: 100, // default number of buckets
		tx:         tx,
		idxName:    idxName,
		idxLayout:  idxLayout,
	}, nil
}

type StaticHashIndex struct {
	NumBuckets int

	tx        transaction.Transaction
	idxName   string
	idxLayout *record.Layout
	searchKey *record.Constant
	ts        *record.TableScan
}

func (hi *StaticHashIndex) SearchCost(numblocks, rpb int) int {
	return 3 + (numblocks / (2 * rpb))
}

func (hi *StaticHashIndex) BeforeFirst(searchkey *record.Constant) error {
	if err := hi.Close(); err != nil {
		return err
	}

	hi.searchKey = searchkey
	bucket := searchkey.Hash() % hi.NumBuckets
	tblname := fmt.Sprintf("%s%d", hi.idxName, bucket)
	ts, err := record.NewTableScan(hi.tx, tblname, hi.idxLayout)
	if err != nil {
		return err
	}
	hi.ts = ts
	return nil
}

func (hi *StaticHashIndex) Next() bool {
	if hi.ts == nil {
		return false
	}
	for hi.ts.Next() {
		dataval, err := hi.ts.GetVal("dataval")
		if err != nil {
			panic(err)
		}
		if dataval.Equal(*hi.searchKey) {
			return true
		}
	}
	return false
}

func (hi *StaticHashIndex) GetDataRID() (*record.RID, error) {
	blknum, err := hi.ts.GetInt("block")
	if err != nil {
		return nil, err
	}
	slot, err := hi.ts.GetInt("id")
	if err != nil {
		return nil, err
	}
	return &record.RID{Blknum: int(blknum), Slot: int(slot)}, nil
}

func (hi *StaticHashIndex) Insert(val *record.Constant, rid *record.RID) error {
	if err := hi.BeforeFirst(val); err != nil {
		return err
	}
	if err := hi.ts.Insert(); err != nil {
		return err
	}
	if err := hi.ts.SetInt("block", rid.Blknum); err != nil {
		return err
	}
	if err := hi.ts.SetInt("id", rid.Slot); err != nil {
		return err
	}
	if err := hi.ts.SetVal("dataval", *val); err != nil {
		return err
	}
	return nil
}

func (hi *StaticHashIndex) Delete(val *record.Constant, rid *record.RID) error {
	err := hi.BeforeFirst(val)
	if err != nil {
		return err
	}

	for hi.Next() {
		currRID, err := hi.GetDataRID()
		if err != nil {
			return err
		}
		if currRID.Equal(*rid) {
			return hi.ts.Delete()
		}
	}
	return nil
}

func (hi *StaticHashIndex) Close() (err error) {
	if hi.ts != nil {
		err = hi.ts.Close()
		hi.ts = nil
	}
	return
}
