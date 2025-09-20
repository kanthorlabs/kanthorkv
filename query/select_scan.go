package query

import "github.com/kanthorlabs/kanthorkv/record"

var _ record.Scan = (*SelectScan)(nil)
var _ record.UpdateScan = (*SelectScan)(nil)

func NewSelectScan(s record.Scan, p *Predicate) (*SelectScan, error) {
	return &SelectScan{s: s, pred: p}, nil
}

type SelectScan struct {
	s    record.Scan
	pred *Predicate
}

func (s *SelectScan) BeforeFirst() error {
	return s.s.BeforeFirst()
}

func (s *SelectScan) Next() bool {
	for s.s.Next() {
		if ok, err := s.pred.IsSatisfied(s.s); err == nil && ok {
			return true
		}
	}
	return false
}

func (s *SelectScan) GetInt(fldname string) (int, error) {
	return s.s.GetInt(fldname)
}

func (s *SelectScan) GetString(fldname string) (string, error) {
	return s.s.GetString(fldname)
}

func (s *SelectScan) GetVal(fldname string) (record.Constant, error) {
	return s.s.GetVal(fldname)
}

func (s *SelectScan) HasField(fldname string) bool {
	return s.s.HasField(fldname)
}

func (s *SelectScan) Close() error {
	return s.s.Close()
}

// UpdateScan methods

func (s *SelectScan) SetInt(fldname string, val int) error {
	us := s.s.(record.UpdateScan)
	return us.SetInt(fldname, val)
}

func (s *SelectScan) SetString(fldname string, val string) error {
	us := s.s.(record.UpdateScan)
	return us.SetString(fldname, val)
}

func (s *SelectScan) SetVal(fldname string, val record.Constant) error {
	us := s.s.(record.UpdateScan)
	return us.SetVal(fldname, val)
}

func (s *SelectScan) Delete() error {
	us := s.s.(record.UpdateScan)
	return us.Delete()
}

func (s *SelectScan) Insert() error {
	us := s.s.(record.UpdateScan)
	return us.Insert()
}

func (s *SelectScan) GetRid() record.RID {
	us := s.s.(record.UpdateScan)
	return us.GetRid()
}

func (s *SelectScan) MoveToRid(rid record.RID) error {
	us := s.s.(record.UpdateScan)
	return us.MoveToRid(rid)
}
