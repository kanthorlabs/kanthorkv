package query

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/record"
)

type Term struct {
	lhs Expression
	rhs Expression
}

func (t *Term) IsSatisfied(s record.Scan) (bool, error) {
	lhsval, err := t.lhs.Evaluate(s)
	if err != nil {
		return false, err
	}
	rhsval, err := t.rhs.Evaluate(s)
	if err != nil {
		return false, err
	}

	return lhsval.Equal(rhsval), nil

}

func (t *Term) ReductionFactor(p Plan) (int, error) {
	if t.lhs.FieldName() != nil && t.rhs.FieldName() != nil {
		lhsname := *t.lhs.FieldName()
		rhsname := *t.rhs.FieldName()
		return max(p.DistinctValues(lhsname), p.DistinctValues(rhsname)), nil
	}

	if t.lhs.FieldName() != nil {
		lhsname := *t.lhs.FieldName()
		return p.DistinctValues(lhsname), nil
	}

	if t.rhs.FieldName() != nil {
		rhsname := *t.rhs.FieldName()
		return p.DistinctValues(rhsname), nil
	}
	if t.lhs.Constant() != nil && t.rhs.Constant() != nil {
		if t.lhs.Constant().Equal(*t.rhs.Constant()) {
			return 1, nil
		}
	}

	return 0, fmt.Errorf("cannot calculate reduction factor for term %s", t.String())
}

func (t *Term) EquatesWithConstant(fldname string) *record.Constant {
	if t.lhs.FieldName() != nil && *t.lhs.FieldName() == fldname && t.rhs.FieldName() != nil {
		return t.rhs.Constant()
	}

	if t.rhs.FieldName() != nil && *t.rhs.FieldName() == fldname && t.lhs.FieldName() != nil {
		return t.lhs.Constant()
	}

	return nil
}

func (t *Term) EquatesWithField(fldname string) *string {
	if t.lhs.FieldName() != nil && *t.lhs.FieldName() == fldname && t.rhs.FieldName() != nil {
		return t.rhs.FieldName()
	}

	if t.rhs.FieldName() != nil && *t.rhs.FieldName() == fldname && t.lhs.FieldName() != nil {
		return t.lhs.FieldName()
	}

	return nil
}

func (t *Term) AppliesTo(sch *record.Schema) bool {
	return t.lhs.AppliesTo(sch) && t.rhs.AppliesTo(sch)
}

func (t *Term) String() string {
	return fmt.Sprintf("%s = %s", t.lhs.String(), t.rhs.String())
}
