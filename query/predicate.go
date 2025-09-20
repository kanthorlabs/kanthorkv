package query

import (
	"strings"

	"github.com/kanthorlabs/kanthorkv/record"
)

func NewPredicate(terms ...*Term) *Predicate {
	return &Predicate{terms: terms}
}

type Predicate struct {
	terms []*Term
}

func (p *Predicate) ConjoinWith(predicate *Predicate) {
	p.terms = append(p.terms, predicate.terms...)
}

func (p *Predicate) IsSatisfied(s record.Scan) (bool, error) {
	for _, t := range p.terms {
		satisfied, err := t.IsSatisfied(s)
		if err != nil || !satisfied {
			return false, err
		}
	}
	return true, nil
}

func (p *Predicate) ReductionFactor(plan Plan) (int, error) {
	factor := 1
	for _, t := range p.terms {
		f, err := t.ReductionFactor(plan)
		if err != nil {
			return 0, err
		}
		factor *= f
	}

	return factor, nil
}

// SelectSubPred returns the subpredicate that applies to the specified schema,
// or nil if the predicate does not apply to the schema.
func (p *Predicate) SelectSubPred(sch *record.Schema) *Predicate {
	result := NewPredicate()
	for _, term := range p.terms {
		if term.AppliesTo(sch) {
			result.terms = append(result.terms, term)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

// JoinSubPred returns the subpredicate consisting of terms that apply to
// the union of the two specified schemas, but not to either
// schema separately.
func (p *Predicate) JoinSubPred(sch1 *record.Schema, sch2 *record.Schema) *Predicate {
	result := NewPredicate()
	newsch := record.NewSchema()
	newsch.AddAll(sch1)
	newsch.AddAll(sch2)
	for _, t := range p.terms {
		if !t.AppliesTo(sch1) && !t.AppliesTo(sch2) && t.AppliesTo(newsch) {
			result.terms = append(result.terms, t)
		}
	}
	if len(result.terms) == 0 {
		return nil
	}
	return result
}

// EquatesWithConstant returns true if the predicate has a term of the form
// "F=c" where F is a field name and c is a constant.
// If so, the method returns the constant, otherwise it returns nil.
func (p *Predicate) EquatesWithConstant(fldname string) *record.Constant {
	for _, t := range p.terms {
		if c := t.EquatesWithConstant(fldname); c != nil {
			return c
		}
	}
	return nil
}

// EquatesWithField returns true if the predicate has a term of the form
// "F1=F2" where F1 is a field name and F2 is some other field name.
// If so, the method returns the field name F2, otherwise it returns nil.
func (p *Predicate) EquatesWithField(fldname string) *string {
	for _, t := range p.terms {
		if f := t.EquatesWithField(fldname); f != nil {
			return f
		}
	}
	return nil
}

// String returns a string representation of this predicate.
func (p *Predicate) String() string {
	terms := make([]string, len(p.terms))
	for i, term := range p.terms {
		terms[i] = term.String()
	}
	return strings.Join(terms, " AND ")
}
