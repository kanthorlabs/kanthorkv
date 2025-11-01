package plan

import (
	"math"

	"github.com/kanthorlabs/kanthorkv/query"
	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

func NewMaterializePlan(tx transaction.Transaction, srcplan query.Plan) *MaterializePlan {
	return &MaterializePlan{
		srcplan: srcplan,
		tx:      tx,
	}
}

var _ query.Plan = (*MaterializePlan)(nil)

type MaterializePlan struct {
	srcplan query.Plan
	tx      transaction.Transaction
}

func (p *MaterializePlan) Open() (record.Scan, error) {
	sch := p.srcplan.Schema()
	temp := query.NewTempTable(p.tx, sch)
	src, err := p.srcplan.Open()
	if err != nil {
		return nil, err
	}
	defer src.Close()

	dest, err := temp.Open()
	if err != nil {
		return nil, err
	}

	for src.Next() {
		if err = dest.Insert(); err != nil {
			return nil, err
		}

		for _, fieldName := range sch.Fields() {
			val, err := src.GetVal(fieldName)
			if err != nil {
				return nil, err
			}
			err = dest.SetVal(fieldName, val)
			if err != nil {
				return nil, err
			}
		}
	}

	if err := dest.BeforeFirst(); err != nil {
		return nil, err
	}

	return dest, nil
}

func (p *MaterializePlan) BlocksAccessed() int {
	layout := record.NewLayoutOfSchema(p.srcplan.Schema())
	rpb := float64(p.tx.BlockSize()) / float64(layout.SlotSize())
	blocksAccessed := int(math.Ceil(float64(p.srcplan.RecordsOutput()) / rpb))
	return blocksAccessed
}

func (p *MaterializePlan) RecordsOutput() int {
	return p.srcplan.RecordsOutput()
}

func (p *MaterializePlan) DistinctValues(fieldName string) int {
	return p.srcplan.DistinctValues(fieldName)
}

func (p *MaterializePlan) Schema() *record.Schema {
	return p.srcplan.Schema()
}
