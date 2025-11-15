package query

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/record"
)

var _ AggregationFn = (*MinFn)(nil)

type MinFn struct {
	fieldName string
	val       record.Constant
}

func NewMinFn(fieldName string) *MinFn {
	return &MinFn{fieldName: fieldName, val: record.Constant{}}
}

func (mf *MinFn) ProcessFirst(scan record.Scan) error {
	val, err := scan.GetVal(mf.fieldName)
	if err != nil {
		return err
	}
	mf.val = val
	return nil
}

func (mf *MinFn) ProcessNext(scan record.Scan) error {
	val, err := scan.GetVal(mf.fieldName)
	if err != nil {
		return err
	}
	if val.Compare(mf.val) < 0 {
		mf.val = val
	}
	return nil
}

func (mf *MinFn) FieldName() string {
	return fmt.Sprintf("max(%s)", mf.fieldName)
}

func (mf *MinFn) Value() record.Constant {
	return mf.val
}
