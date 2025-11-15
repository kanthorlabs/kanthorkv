package query

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/record"
)

type GroupValue struct {
	vals map[string]record.Constant
}

func NewGroupValue(scan record.Scan, fields []string) (*GroupValue, error) {
	vals := make(map[string]record.Constant)
	for _, field := range fields {
		val, err := scan.GetVal(field)
		if err != nil {
			return nil, err
		}
		vals[field] = val
	}
	return &GroupValue{vals: vals}, nil
}

func (gv *GroupValue) GetVal(fieldName string) (record.Constant, error) {
	val, ok := gv.vals[fieldName]
	if !ok {
		return record.Constant{}, fmt.Errorf("field %s not found", fieldName)
	}
	return val, nil
}

func (gv *GroupValue) Equals(other *GroupValue) bool {
	if len(gv.vals) != len(other.vals) {
		return false
	}
	for fieldName, val := range gv.vals {
		otherVal, ok := other.vals[fieldName]
		if !ok {
			return false
		}
		if !val.Equal(otherVal) {
			return false
		}
	}
	return true
}
