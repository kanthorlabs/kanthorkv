package query

import (
	"fmt"
	"slices"

	"github.com/kanthorlabs/kanthorkv/record"
)

var _ record.Scan = (*GroupByScan)(nil)

type GroupByScan struct {
	scan        *SortScan
	groupFields []string
	aggFns      []AggregationFn
	groupValue  *GroupValue
	hasMore     bool
}

func NewGroupByScan(scan *SortScan, groupFields []string, aggFns []AggregationFn) *GroupByScan {
	return &GroupByScan{
		scan:        scan,
		groupFields: groupFields,
		aggFns:      aggFns,
		groupValue:  nil,
		hasMore:     false,
	}
}

func (gs *GroupByScan) BeforeFirst() error {
	if err := gs.scan.BeforeFirst(); err != nil {
		return err
	}
	gs.hasMore = gs.scan.Next()
	return nil
}

func (gs *GroupByScan) Next() bool {
	if !gs.hasMore {
		return false
	}

	for _, fn := range gs.aggFns {
		if err := fn.ProcessFirst(gs.scan); err != nil {
			panic(err)
		}
	}
	groupValue, err := NewGroupValue(gs.scan, gs.groupFields)
	if err != nil {
		panic(err)
	}
	gs.groupValue = groupValue

	for gs.hasMore = gs.scan.Next(); gs.hasMore; {
		gv, err := NewGroupValue(gs.scan, gs.groupFields)
		if err != nil {
			panic(err)
		}
		if !gs.groupValue.Equals(gv) {
			break
		}
		for _, fn := range gs.aggFns {
			if err := fn.ProcessNext(gs.scan); err != nil {
				panic(err)
			}
		}
	}

	return true
}

func (gs *GroupByScan) Close() error {
	return gs.scan.Close()
}

func (gs *GroupByScan) GetVal(fieldName string) (record.Constant, error) {
	if slices.Contains(gs.groupFields, fieldName) {
		return gs.groupValue.GetVal(fieldName)
	}
	for _, fn := range gs.aggFns {
		if fieldName == fn.FieldName() {
			return fn.Value(), nil
		}
	}
	return record.Constant{}, fmt.Errorf("field %s not found", fieldName)
}

func (gs *GroupByScan) GetInt(fieldName string) (int, error) {
	val, err := gs.GetVal(fieldName)
	if err != nil {
		return 0, fmt.Errorf("gs.GetVal(%s): %w", fieldName, err)
	}
	return val.AsInt(), nil
}

func (gs *GroupByScan) GetString(fieldName string) (string, error) {
	val, err := gs.GetVal(fieldName)
	if err != nil {
		return "", fmt.Errorf("gs.GetVal(%s): %w", fieldName, err)
	}
	return val.AsString(), nil
}

func (gs *GroupByScan) HasField(fieldName string) bool {
	if slices.Contains(gs.groupFields, fieldName) {
		return true
	}
	for _, fn := range gs.aggFns {
		if fieldName == fn.FieldName() {
			return true
		}
	}
	return false
}
