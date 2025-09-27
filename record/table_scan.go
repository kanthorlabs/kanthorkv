package record

import (
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

// Scan is the interface for scanning through a table.
type Scan interface {
	// BeforeFirst positions the scan before the first record.
	// A subsequent call to Next() will return the first record.
	BeforeFirst() error
	// Next moves the scan to the next record.
	// Returns false if there is no next record.
	Next() bool
	// GetInt returns the value of the specified integer field in the current record.
	GetInt(fldname string) (int, error)
	// GetString returns the value of the specified string field in the current record.
	GetString(fldname string) (string, error)
	// GetVal returns the value of the specified field in the current record as a Constant.
	GetVal(fldname string) (Constant, error)
	// HasField returns true if the scan has a field with the specified name.
	HasField(fldname string) bool
	// Close closes the scan and its subscans, if any.
	Close() error
}

// UpdateScan is the interface for all updateable scans.
type UpdateScan interface {
	Scan
	// SetVal modifies the field value of the current record.
	SetVal(fldname string, val Constant) error
	// SetInt modifies the field value of the current record.
	SetInt(fldname string, val int) error
	// SetString modifies the field value of the current record.
	SetString(fldname string, val string) error
	// Insert inserts a new record somewhere in the scan.
	Insert() error
	// Delete deletes the current record.
	Delete() error
	// GetRid returns the RID of the current record.
	GetRid() RID
	// MoveToRid positions the scan so that the current record has the specified RID.
	MoveToRid(rid RID) error
}

func NewTableScan(tx transaction.Transaction, tblname string, layout *Layout) (*TableScan, error) {
	ts := &TableScan{
		tx:       tx,
		layout:   layout,
		filename: tblname + ".tbl",
	}

	size, err := tx.Size(ts.filename)
	if err != nil {
		return nil, err
	}
	if size == 0 {
		ts.moveToNewBlock()
	} else {
		ts.moveToBlock(0)
	}

	return ts, nil
}

var _ UpdateScan = (*TableScan)(nil)

type TableScan struct {
	tx          transaction.Transaction
	layout      *Layout
	rp          *RecordPage
	filename    string
	currentslot int
}

func (ts *TableScan) BeforeFirst() error {
	return ts.moveToBlock(0)
}

func (ts *TableScan) Next() bool {
	ts.currentslot = ts.rp.NextAfter(ts.currentslot)
	if ts.currentslot < 0 {
		if ts.atLastBlock() {
			return false
		}

		if err := ts.moveToBlock(ts.rp.Block().Number() + 1); err != nil {
			panic(err)
		}
		// Reset the current slot to the first slot in the new block.
		ts.currentslot = ts.rp.NextAfter(ts.currentslot)
	}

	return true
}

func (ts *TableScan) GetInt(fldname string) (int, error) {
	return ts.rp.GetInt(ts.currentslot, fldname)
}

func (ts *TableScan) GetString(fldname string) (string, error) {
	return ts.rp.GetString(ts.currentslot, fldname)
}

func (ts *TableScan) GetVal(fldname string) (Constant, error) {
	if ts.layout.Schema().Type(fldname) == IntegerField {
		i, err := ts.GetInt(fldname)
		if err != nil {
			return Constant{}, err
		}
		return NewIntConstant(i), nil
	}

	s, err := ts.GetString(fldname)
	if err != nil {
		return Constant{}, err
	}
	return NewStringConstant(s), nil
}

func (ts *TableScan) HasField(fldname string) bool {
	return ts.layout.sch.HasField(fldname)
}

func (ts *TableScan) Close() error {
	return ts.tx.Unpin(ts.rp.Block())
}

func (ts *TableScan) SetInt(fldname string, val int) error {
	return ts.rp.SetInt(ts.currentslot, fldname, val)
}

func (ts *TableScan) SetString(fldname string, val string) error {
	return ts.rp.SetString(ts.currentslot, fldname, val)
}

func (ts *TableScan) SetVal(fldname string, val Constant) error {
	if ts.layout.Schema().Type(fldname) == IntegerField {
		return ts.SetInt(fldname, val.AsInt())
	}

	return ts.SetString(fldname, val.AsString())
}

func (ts *TableScan) Insert() error {
	ts.currentslot = ts.rp.InsertAfter(ts.currentslot)
	for ts.currentslot < 0 {
		if ts.atLastBlock() {
			ts.moveToNewBlock()
		} else {
			ts.moveToBlock(ts.rp.Block().Number() + 1)
		}
		ts.currentslot = ts.rp.InsertAfter(ts.currentslot)
	}
	return nil
}

func (ts *TableScan) Delete() error {
	return ts.rp.Delete(ts.currentslot)
}

func (ts *TableScan) MoveToRid(rid RID) error {
	if err := ts.Close(); err != nil {
		return err
	}

	blk := file.NewBlockId(ts.filename, rid.BlockNumber())
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.currentslot = -1
	return nil
}

func (ts *TableScan) GetRid() RID {
	return NewRID(ts.rp.Block().Number(), ts.currentslot)
}

func (ts *TableScan) moveToBlock(blknum int) error {
	if err := ts.Close(); err != nil {
		return err
	}

	blk := file.NewBlockId(ts.filename, blknum)
	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.currentslot = -1
	return nil
}

func (ts *TableScan) moveToNewBlock() error {
	if err := ts.Close(); err != nil {
		return err
	}

	blk, err := ts.tx.Append(ts.filename)
	if err != nil {
		return err
	}

	ts.rp = NewRecordPage(ts.tx, blk, ts.layout)
	ts.rp.Format()
	ts.currentslot = -1
	return nil
}

func (ts *TableScan) atLastBlock() bool {
	size, err := ts.tx.Size(ts.filename)
	if err != nil {
		panic(err)
	}
	return ts.rp.Block().Number() == size-1
}
