package query

import (
	"github.com/kanthorlabs/kanthorkv/record"
)

func NewConstantExpression(val *record.Constant) *Expression {
	return &Expression{val: val}
}

func NewFieldExpression(fldname *string) *Expression {
	return &Expression{fldname: fldname}
}

type Expression struct {
	val     *record.Constant // using pointer to represent nullable constant
	fldname *string          // using pointer to represent nullable string
}

func (e *Expression) Evaluate(s record.Scan) (record.Constant, error) {
	if e.val != nil {
		return *e.val, nil
	}
	return s.GetVal(*e.fldname)
}

func (e *Expression) Constant() *record.Constant {
	return e.val
}

func (e *Expression) FieldName() *string {
	return e.fldname
}

func (e *Expression) AppliesTo(sch *record.Schema) bool {
	if e.val != nil {
		return true
	}
	return sch.HasField(*e.fldname)
}

func (e *Expression) String() string {
	if e.val != nil {
		return e.val.String()
	}
	return *e.fldname
}
