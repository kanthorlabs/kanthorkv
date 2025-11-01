package plan

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/query"
	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

func NewSortPlan(tx transaction.Transaction, plan query.Plan, sortFields []string) (*SortPlan, error) {
	return &SortPlan{

		plan:   plan,
		tx:     tx,
		schema: plan.Schema(),
		comp:   query.NewRecordComparator(sortFields),
	}, nil
}

var _ query.Plan = (*SortPlan)(nil)

type SortPlan struct {
	plan   query.Plan
	tx     transaction.Transaction
	schema *record.Schema
	comp   *query.RecordComparator
}

func (sp *SortPlan) Open() (record.Scan, error) {
	src, err := sp.plan.Open()
	if err != nil {
		return nil, err
	}
	runs, err := sp.splitIntoRuns(src)
	if err != nil {
		return nil, err
	}
	if err := src.Close(); err != nil {
		return nil, err
	}

	for len(runs) > 2 {
		runs, err = sp.doAMergeIteration(runs)
		if err != nil {
			return nil, err
		}
	}

	sc, err := query.NewSortScan(runs, sp.comp)
	if err != nil {
		return nil, err
	}
	return sc, nil
}

func (sp *SortPlan) BlocksAccessed() int {
	mp := NewMaterializePlan(sp.tx, sp.plan)
	return mp.BlocksAccessed()
}

func (sp *SortPlan) RecordsOutput() int {
	return sp.plan.RecordsOutput()
}

func (sp *SortPlan) DistinctValues(fieldName string) int {
	return sp.plan.DistinctValues(fieldName)
}

func (sp *SortPlan) Schema() *record.Schema {
	return sp.schema
}

// splitIntoRuns splits the source scan into sorted runs with original order preserved.
// Lets say we have [11, 5, 7, 2, 9, 1, 8]
// This would be split into the following runs:
// [11], [5, 7], [2, 9], [1, 8]
func (sp *SortPlan) splitIntoRuns(src record.Scan) ([]*query.TempTable, error) {
	temps := make([]*query.TempTable, 0)
	err := src.BeforeFirst()
	if err != nil {
		return nil, fmt.Errorf("src.BeforeFirst: %w", err)
	}

	currentTemp := query.NewTempTable(sp.tx, sp.schema)
	temps = append(temps, currentTemp)
	currentScan, err := currentTemp.Open()
	if err != nil {
		return nil, err
	}

	for src.Next() {
		next, err := sp.copy(src, currentScan)
		if err != nil {
			return nil, err
		}
		if !next {
			break
		}

		cmp, err := sp.comp.Compare(src, currentScan)
		if err != nil {
			return nil, err
		}
		// src < currentScan
		// we've reached the end of a sorted run.
		if cmp < 0 {
			if err := currentScan.Close(); err != nil {
				return nil, err
			}
			currentTemp = query.NewTempTable(sp.tx, sp.schema)
			temps = append(temps, currentTemp)
			currentScan, err = currentTemp.Open()
			if err != nil {
				return nil, err
			}
		}
	}

	return temps, currentScan.Close()
}

func (sp *SortPlan) copy(src record.Scan, dest record.UpdateScan) (bool, error) {
	if err := dest.Insert(); err != nil {
		return false, err
	}

	for _, fieldName := range sp.schema.Fields() {
		val, err := src.GetVal(fieldName)
		if err != nil {
			return false, err
		}

		if err := dest.SetVal(fieldName, val); err != nil {
			return false, err
		}
	}

	return src.Next(), nil
}

func (sp *SortPlan) doAMergeIteration(runs []*query.TempTable) ([]*query.TempTable, error) {
	result := make([]*query.TempTable, 0)
	for len(runs) > 1 {
		p1 := runs[0]
		p2 := runs[1]
		runs = runs[2:]
		merged, err := sp.mergeTwoRuns(p1, p2)
		if err != nil {
			return nil, err
		}
		result = append(result, merged)
	}

	if len(runs) == 1 {
		result = append(result, runs[0])
	}
	return result, nil
}

func (sp *SortPlan) mergeTwoRuns(p1 *query.TempTable, p2 *query.TempTable) (*query.TempTable, error) {
	src1, err := p1.Open()
	if err != nil {
		return nil, err
	}
	defer src1.Close()

	src2, err := p2.Open()
	if err != nil {
		return nil, err
	}
	defer src2.Close()

	result := query.NewTempTable(sp.tx, sp.schema)
	dest, err := result.Open()
	if err != nil {
		return nil, err
	}
	defer dest.Close()

	var hasMore1, hasMore2 bool
	for src1.Next() && src2.Next() {
		cmp, err := sp.comp.Compare(src1, src2)
		if err != nil {
			return nil, err
		}
		if cmp < 0 {
			hasMore1, err = sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
		} else {
			hasMore2, err = sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
		}
	}

	if hasMore1 {
		for hasMore1 {
			hasMore1, err = sp.copy(src1, dest)
			if err != nil {
				return nil, err
			}
		}
	} else {
		for hasMore2 {
			hasMore2, err = sp.copy(src2, dest)
			if err != nil {
				return nil, err
			}
		}
	}

	return result, nil
}
