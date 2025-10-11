package plan

import (
	"github.com/kanthorlabs/kanthorkv/query"
	"github.com/kanthorlabs/kanthorkv/record"
)

var _ query.Plan = (*ProductPlan)(nil)

// NewProductPlan creates a new ProductPlan with the specified subqueries.
func NewProductPlan(p1, p2 query.Plan) *ProductPlan {
	schema := record.NewSchema()
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())
	return &ProductPlan{p1: p1, p2: p2, schema: schema}
}

type ProductPlan struct {
	p1, p2 query.Plan
	schema *record.Schema
}

func (pp *ProductPlan) Open() (record.Scan, error) {
	s1, err := pp.p1.Open()
	if err != nil {
		return nil, err
	}
	s2, err := pp.p2.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProductScan(s1, s2)
}

func (pp *ProductPlan) BlocksAccessed() int {
	return pp.p1.BlocksAccessed() + (pp.p1.RecordsOutput() * pp.p2.BlocksAccessed())
}

func (pp *ProductPlan) RecordsOutput() int {
	return pp.p1.RecordsOutput() * pp.p2.RecordsOutput()
}

func (pp *ProductPlan) DistinctValues(fldname string) int {
	if pp.p1.Schema().HasField(fldname) {
		return pp.p1.DistinctValues(fldname)
	}
	return pp.p2.DistinctValues(fldname)
}

func (pp *ProductPlan) Schema() *record.Schema {
	return pp.schema
}
