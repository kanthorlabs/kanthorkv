package plan

import (
	"github.com/kanthorlabs/kanthorkv/metadata"
	"github.com/kanthorlabs/kanthorkv/parser"
	"github.com/kanthorlabs/kanthorkv/query"
	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

// BasicUpdatePlanner is a basic planner for SQL update statements.
type BasicUpdatePlanner struct {
	mdm *metadata.MetadataMgr
}

var _ UpdatePlanner = (*BasicUpdatePlanner)(nil)

// NewBasicUpdatePlanner creates a new BasicUpdatePlanner.
func NewBasicUpdatePlanner(mdm *metadata.MetadataMgr) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{mdm: mdm}
}

func (p *BasicUpdatePlanner) ExecuteDelete(data *parser.DeleteData, tx transaction.Transaction) (int, error) {
	var plan query.Plan
	plan, err := NewTablePlan(data.TableName, tx, p.mdm)
	if err != nil {
		return 0, err
	}

	plan = NewSelectPlan(plan, data.Pred)
	s, err := plan.Open()
	if err != nil {
		return 0, err
	}

	// SelectPlan use SelectScan, that is implementation of UpdateScan
	us, count := s.(record.UpdateScan), 0
	for us.Next() {
		if err := us.Delete(); err != nil {
			return 0, err
		}
		count++
	}

	return count, us.Close()
}

func (p *BasicUpdatePlanner) ExecuteUpdate(data *parser.UpdateData, tx transaction.Transaction) (int, error) {
	var plan query.Plan
	plan, err := NewTablePlan(data.TableName, tx, p.mdm)
	if err != nil {
		return 0, err
	}

	plan = NewSelectPlan(plan, data.Pred)
	s, err := plan.Open()
	if err != nil {
		return 0, err
	}

	// SelectPlan use SelectScan, that is implementation of UpdateScan
	us, count := s.(record.UpdateScan), 0
	for us.Next() {
		val, err := data.NewValue.Evaluate(us)
		if err != nil {
			return 0, err
		}
		err = us.SetVal(data.TargetField, val)
		if err != nil {
			return 0, err
		}
		count++
	}

	return count, us.Close()
}

func (p *BasicUpdatePlanner) ExecuteInsert(data *parser.InsertData, tx transaction.Transaction) (int, error) {
	plan, err := NewTablePlan(data.TableName, tx, p.mdm)
	if err != nil {
		return 0, err
	}
	s, err := plan.Open()
	if err != nil {
		return 0, err
	}
	us := s.(record.UpdateScan)
	// take the slot first
	if err = us.Insert(); err != nil {
		return 0, err
	}

	for i, val := range data.Values {
		if err = us.SetVal(data.Fields[i], val); err != nil {
			return 0, err
		}
	}

	// we inserted one record
	return 1, us.Close()
}

func (p *BasicUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, tx transaction.Transaction) (int, error) {
	if err := p.mdm.CreateTable(data.TableName, data.Schema, tx); err != nil {
		return 0, err
	}
	return 0, nil
}

func (p *BasicUpdatePlanner) ExecuteCreateView(data *parser.CreateViewData, tx transaction.Transaction) (int, error) {
	if err := p.mdm.CreateView(data.ViewName, data.QueryData.String(), tx); err != nil {
		return 0, err
	}
	return 0, nil
}

func (p *BasicUpdatePlanner) ExecuteCreateIndex(data *parser.CreateIndexData, tx transaction.Transaction) (int, error) {
	if err := p.mdm.CreateIndex(data.IndexName, data.TableName, data.FieldName, tx); err != nil {
		return 0, err
	}
	return 0, nil
}
