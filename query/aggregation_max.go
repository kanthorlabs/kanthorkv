package query

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/record"
)

var _ AggregationFn = (*MaxFn)(nil)

type MaxFn struct {
	fieldName string
	val       record.Constant
}

func NewMaxFn(fieldName string) *MaxFn {
	return &MaxFn{fieldName: fieldName, val: record.Constant{}}
}

func (mf *MaxFn) ProcessFirst(scan record.Scan) error {
	val, err := scan.GetVal(mf.fieldName)
	if err != nil {
		return err
	}
	mf.val = val
	return nil
}

func (mf *MaxFn) ProcessNext(scan record.Scan) error {
	val, err := scan.GetVal(mf.fieldName)
	if err != nil {
		return err
	}
	if val.Compare(mf.val) > 0 {
		mf.val = val
	}
	return nil
}

func (mf *MaxFn) FieldName() string {
	return fmt.Sprintf("max(%s)", mf.fieldName)
}

func (mf *MaxFn) Value() record.Constant {
	return mf.val
}
