package plan

import (
	"errors"

	"github.com/kanthorlabs/kanthorkv/parser"
	"github.com/kanthorlabs/kanthorkv/query"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

// QueryPlanner is the interface implemented by planners for the
// SQL select statement.
type QueryPlanner interface {
	// CreatePlan creates a plan for the parsed query.
	CreatePlan(data *parser.QueryData, tx transaction.Transaction) (query.Plan, error)
}

// UpdatePlanner is the interface implemented by planners for the
// SQL update statement.
type UpdatePlanner interface {
	// ExecuteInsert creates a plan for an insert statement,
	// returning the number of affected records.
	ExecuteInsert(data *parser.InsertData, tx transaction.Transaction) (int, error)

	// ExecuteDelete creates a plan for a delete statement,
	// returning the number of affected records.
	ExecuteDelete(data *parser.DeleteData, tx transaction.Transaction) (int, error)

	// ExecuteUpdate creates a plan for an update statement,
	// returning the number of affected records.
	ExecuteUpdate(data *parser.UpdateData, tx transaction.Transaction) (int, error)

	// ExecuteCreateTable creates a plan for a create table statement,
	// returning the number of affected records.
	ExecuteCreateTable(data *parser.CreateTableData, tx transaction.Transaction) (int, error)

	// ExecuteCreateView creates a plan for a create view statement,
	// returning the number of affected records.
	ExecuteCreateView(data *parser.CreateViewData, tx transaction.Transaction) (int, error)

	// ExecuteCreateIndex creates a plan for a create index statement,
	// returning the number of affected records.
	ExecuteCreateIndex(data *parser.CreateIndexData, tx transaction.Transaction) (int, error)
}

// Planner executes SQL statements.
type Planner struct {
	qp QueryPlanner
	up UpdatePlanner
}

// NewPlanner creates a new Planner.
func NewPlanner(qp QueryPlanner, up UpdatePlanner) *Planner {
	return &Planner{qp: qp, up: up}
}

// CreateQueryPlan creates a query plan for the given SQL query.
func (p *Planner) CreateQueryPlan(query string, tx transaction.Transaction) (query.Plan, error) {
	lexer := parser.NewLexer(query)
	ps := parser.New(lexer)
	data, err := ps.Query()
	if err != nil {
		return nil, err
	}
	return p.qp.CreatePlan(data, tx)
}

// ExecuteUpdate executes a SQL insert, delete, modify, or create statement.
// The method dispatches to the appropriate method of the supplied
// update planner, depending on what the parser returns.
// It returns the number of records affected by the update.
func (p *Planner) ExecuteUpdate(query string, tx transaction.Transaction) (int, error) {
	lexer := parser.NewLexer(query)
	ps := parser.New(lexer)
	cmd, err := ps.UpdateCmd()
	if err != nil {
		return 0, err
	}
	if insertCmd, ok := cmd.(*parser.InsertData); ok {
		return p.up.ExecuteInsert(insertCmd, tx)
	}
	if deleteCmd, ok := cmd.(*parser.DeleteData); ok {
		return p.up.ExecuteDelete(deleteCmd, tx)
	}
	if updateCmd, ok := cmd.(*parser.UpdateData); ok {
		return p.up.ExecuteUpdate(updateCmd, tx)
	}
	if createTableCmd, ok := cmd.(*parser.CreateTableData); ok {
		return p.up.ExecuteCreateTable(createTableCmd, tx)
	}
	if createViewCmd, ok := cmd.(*parser.CreateViewData); ok {
		return p.up.ExecuteCreateView(createViewCmd, tx)
	}
	if createIndexCmd, ok := cmd.(*parser.CreateIndexData); ok {
		return p.up.ExecuteCreateIndex(createIndexCmd, tx)
	}
	return 0, errors.New("invalid update command")
}
