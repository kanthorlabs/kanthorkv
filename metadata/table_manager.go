package metadata

import (
	"errors"

	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

// table or field name max length
const TABLE_MAX_LEN = 16

func NewTableMgr(isNew bool, tx transaction.Transaction) (*TableMgr, error) {
	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", TABLE_MAX_LEN)
	tcatSchema.AddIntField("slotsize")
	tcatLayout := record.NewLayoutOfSchema(tcatSchema)

	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", TABLE_MAX_LEN)
	fcatSchema.AddStringField("fldname", TABLE_MAX_LEN)
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	fcatLayout := record.NewLayoutOfSchema(fcatSchema)

	tblmgr := &TableMgr{
		tcatLayout: tcatLayout,
		fcatLayout: fcatLayout,
	}

	if isNew {
		if err := tblmgr.CreateTable("tblcat", tcatSchema, tx); err != nil {
			return nil, err
		}
		if err := tblmgr.CreateTable("fldcat", fcatSchema, tx); err != nil {
			return nil, err
		}
	}

	return tblmgr, nil
}

type TableMgr struct {
	tcatLayout *record.Layout
	fcatLayout *record.Layout
}

func (tm *TableMgr) CreateTable(tblname string, sch *record.Schema, tx transaction.Transaction) (err error) {
	layout := record.NewLayoutOfSchema(sch)
	// insert one record into table cat
	tcat, err := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, tcat.Close())
	}()

	if err := tcat.Insert(); err != nil {
		return err
	}
	if err := tcat.SetString("tblname", tblname); err != nil {
		return err
	}
	if err := tcat.SetInt("slotsize", layout.SlotSize()); err != nil {
		return err
	}

	// inser a record into fldcat for each field
	fcat, err := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, fcat.Close())
	}()

	for _, fldname := range sch.Fields() {
		if err := fcat.Insert(); err != nil {
			return err
		}
		if err := fcat.SetString("tblname", tblname); err != nil {
			return err
		}
		if err := fcat.SetString("fldname", fldname); err != nil {
			return err
		}
		if err := fcat.SetInt("type", int(sch.Type(fldname))); err != nil {
			return err
		}
		if err := fcat.SetInt("length", sch.Length(fldname)); err != nil {
			return err
		}
		if err := fcat.SetInt("offset", layout.Offset(fldname)); err != nil {
			return err
		}
	}

	return nil
}

func (tm *TableMgr) GetLayout(tname string, tx transaction.Transaction) (*record.Layout, error) {
	size := -1
	tcat, err := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, tcat.Close())
	}()

	for tcat.Next() {
		tblname, err := tcat.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if tblname == tname {
			size, err = tcat.GetInt("slotsize")
			if err != nil {
				return nil, err
			}
			break
		}
	}

	sch := record.NewSchema()
	offsets := make(map[string]int)
	fcat, err := record.NewTableScan(tx, "fldcat", tm.fcatLayout)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, fcat.Close())
	}()

	for fcat.Next() {
		tblname, err := fcat.GetString("tblname")
		if err != nil {
			return nil, err
		}
		if tblname == tname {
			fldname, err := fcat.GetString("fldname")
			if err != nil {
				return nil, err
			}
			ftype, err := fcat.GetInt("type")
			if err != nil {
				return nil, err
			}
			length, err := fcat.GetInt("length")
			if err != nil {
				return nil, err
			}
			offset, err := fcat.GetInt("offset")
			if err != nil {
				return nil, err
			}
			offsets[fldname] = offset
			sch.AddField(fldname, record.FieldType(ftype), length)
		}
	}

	return record.NewLayout(sch, offsets, size), nil
}
