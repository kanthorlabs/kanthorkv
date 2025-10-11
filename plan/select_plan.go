package plan

import (
	"github.com/kanthorlabs/kanthorkv/query"
	"github.com/kanthorlabs/kanthorkv/record"
)

var _ query.Plan = (*SelectPlan)(nil)

func NewSelectPlan(p query.Plan, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{
		p:    p,
		pred: pred,
	}
}

type SelectPlan struct {
	p    query.Plan
	pred *query.Predicate
}

func (sp *SelectPlan) Open() (record.Scan, error) {
	s, err := sp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewSelectScan(s, sp.pred)
}

func (sp *SelectPlan) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

func (sp *SelectPlan) RecordsOutput() int {
	factor, err := sp.pred.ReductionFactor(sp.p)
	if err != nil {
		return sp.p.RecordsOutput()
	}
	return sp.p.RecordsOutput() / factor
}

// DistinctValues returns an estimate of the number of distinct values
// in the projection.
// If the predicate contains a term equating the specified field to a
// constant, then this value will be 1.
// Otherwise, it will be the number of distinct values in the underlying
// query (but not more than the size of the output table).
func (sp *SelectPlan) DistinctValues(fldname string) int {
	if sp.pred.EquatesWithConstant(fldname) != nil {
		return 1
	}

	f2 := sp.pred.EquatesWithField(fldname)
	if f2 != nil {
		return min(sp.p.DistinctValues(fldname), sp.p.DistinctValues(*f2))
	}

	return sp.p.DistinctValues(fldname)
}

func (sp *SelectPlan) Schema() *record.Schema {
	return sp.p.Schema()
}
