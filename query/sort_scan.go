package query

import (
	"errors"
	"fmt"

	"github.com/kanthorlabs/kanthorkv/record"
)

func NewSortScan(runs []*TempTable, comp *RecordComparator) (*SortScan, error) {
	if !(len(runs) == 1 || len(runs) == 2) {
		return nil, fmt.Errorf("runs must have 1 or 2 elements, but got %d", len(runs))
	}

	s1, err := runs[0].Open()
	if err != nil {
		return nil, err
	}

	var s2 record.UpdateScan
	if len(runs) > 1 {
		s2, err = runs[1].Open()
		if err != nil {
			return nil, err
		}
	}

	return &SortScan{
		s1:          s1,
		s2:          s2,
		currentScan: nil,
		comp:        comp,
		hasMore1:    s1.Next(),
		hasMore2:    s2.Next(),
	}, nil
}

var _ record.Scan = (*SortScan)(nil)

type SortScan struct {
	s1, s2             record.UpdateScan
	currentScan        record.UpdateScan
	comp               *RecordComparator
	hasMore1, hasMore2 bool
	savedPosition      []record.RID
}

func (ss *SortScan) BeforeFirst() error {
	var err error
	if err = ss.s1.BeforeFirst(); err != nil {
		return err
	}
	ss.hasMore1 = ss.s1.Next()

	if ss.s2 != nil {
		if err = ss.s2.BeforeFirst(); err != nil {
			return err
		}
		ss.hasMore2 = ss.s2.Next()
	}

	return nil
}

func (ss *SortScan) Next() bool {
	if ss.currentScan == ss.s1 {
		ss.hasMore1 = ss.s1.Next()
	} else if ss.s2 != nil && ss.currentScan == ss.s2 {
		ss.hasMore2 = ss.s2.Next()
	}

	if !ss.hasMore1 && !ss.hasMore2 {
		return false
	} else if ss.hasMore1 && ss.hasMore2 {
		cmp, err := ss.comp.Compare(ss.s1, ss.s2)
		if err != nil {
			return false
		}
		if cmp < 0 {
			ss.currentScan = ss.s1
		} else {
			ss.currentScan = ss.s2
		}
	} else if ss.hasMore1 {
		ss.currentScan = ss.s1
	} else if ss.hasMore2 {
		ss.currentScan = ss.s2
	}

	return true
}

func (ss *SortScan) Close() error {
	err := ss.s1.Close()
	if ss.s2 == nil {
		return err
	}
	return errors.Join(err, ss.s2.Close())
}

func (ss *SortScan) GetVal(fieldName string) (record.Constant, error) {
	return ss.currentScan.GetVal(fieldName)
}

func (ss *SortScan) GetInt(fieldName string) (int, error) {
	return ss.currentScan.GetInt(fieldName)
}

func (ss *SortScan) GetString(fieldName string) (string, error) {
	return ss.currentScan.GetString(fieldName)
}

func (ss *SortScan) HasField(fieldName string) bool {
	return ss.currentScan.HasField(fieldName)
}

func (ss *SortScan) SavePosition() error {
	rid1 := ss.s1.GetRid()
	if ss.s2 != nil {
		rid2 := ss.s2.GetRid()
		ss.savedPosition = []record.RID{rid1, rid2}
	} else {
		ss.savedPosition = []record.RID{rid1}
	}

	return nil
}

func (ss *SortScan) RestorePosition() error {
	rid1 := ss.savedPosition[0]
	if err := ss.s1.MoveToRid(rid1); err != nil {
		return err
	}

	if len(ss.savedPosition) > 1 {
		rid2 := ss.savedPosition[1]
		if err := ss.s2.MoveToRid(rid2); err != nil {
			return err
		}
	}

	return nil
}
