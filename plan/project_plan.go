package plan

import (
	"github.com/kanthorlabs/kanthorkv/query"
	"github.com/kanthorlabs/kanthorkv/record"
)

var _ query.Plan = (*ProjectPlan)(nil)

func NewProjectPlan(p query.Plan, fieldnames []string) *ProjectPlan {
	schema := record.NewSchema()
	for _, fldname := range fieldnames {
		schema.Add(fldname, p.Schema())
	}

	return &ProjectPlan{
		p:      p,
		schema: schema,
	}
}

type ProjectPlan struct {
	p      query.Plan
	schema *record.Schema
}

func (pp *ProjectPlan) Open() (record.Scan, error) {
	s, err := pp.p.Open()
	if err != nil {
		return nil, err
	}
	return query.NewProjectScan(s, pp.schema.Fields())
}

func (pp *ProjectPlan) BlocksAccessed() int {
	return pp.p.BlocksAccessed()
}

func (pp *ProjectPlan) RecordsOutput() int {
	return pp.p.RecordsOutput()
}

func (pp *ProjectPlan) DistinctValues(fldname string) int {
	return pp.p.DistinctValues(fldname)
}

func (pp *ProjectPlan) Schema() *record.Schema {
	return pp.schema
}
