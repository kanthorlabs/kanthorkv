package record

import (
	"sync"

	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

func NewStatMgr(tblmgr *TableMgr, tx transaction.Transaction) (*StatMgr, error) {
	statmgr := &StatMgr{
		tblmgr:     tblmgr,
		tablestats: make(map[string]*StatInfo),
		numcalls:   0,
	}
	err := statmgr.RefreshStatistics(tx)
	if err != nil {
		return nil, err
	}

	return statmgr, nil
}

type StatMgr struct {
	tblmgr     *TableMgr
	tablestats map[string]*StatInfo
	numcalls   int

	mu  sync.Mutex
	tmu sync.Mutex
}

func (sm *StatMgr) GetStatInfo(tblname string, layout *Layout, tx transaction.Transaction) (*StatInfo, error) {
	sm.numcalls++
	if sm.numcalls > 100 {
		err := sm.RefreshStatistics(tx)
		if err != nil {
			return nil, err
		}
	}

	if si, ok := sm.tablestats[tblname]; ok {
		return si, nil
	}

	si, err := sm.CalcTableStats(tblname, layout, tx)
	if err != nil {
		return nil, err
	}
	sm.tablestats[tblname] = si
	return si, nil
}

func (sm *StatMgr) RefreshStatistics(tx transaction.Transaction) (err error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.numcalls = 0
	sm.tablestats = make(map[string]*StatInfo)

	tcatlayout, err := sm.tblmgr.GetLayout("tblcat", tx)
	if err != nil {
		return err
	}
	tcat, err := NewTableScan(tx, "tblcat", tcatlayout)
	if err != nil {
		return err
	}
	defer func() {
		err = tcat.Close()
	}()

	for tcat.Next() {
		tblname, err := tcat.GetString("tblname")
		if err != nil {
			return err
		}
		layout, err := sm.tblmgr.GetLayout(tblname, tx)
		if err != nil {
			return err
		}
		si, err := sm.CalcTableStats(tblname, layout, tx)
		if err != nil {
			return err
		}
		sm.tablestats[tblname] = si
	}

	return nil
}

func (sm *StatMgr) CalcTableStats(tblname string, layout *Layout, tx transaction.Transaction) (*StatInfo, error) {
	sm.tmu.Lock()
	defer sm.tmu.Unlock()

	numRecs := 0
	numblocks := 0

	ts, err := NewTableScan(tx, tblname, layout)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = ts.Close()
	}()

	for ts.Next() {
		numRecs++
		numblocks = ts.GetRid().BlockNumber() + 1
	}

	return NewStatInfo(numblocks, numRecs), nil
}

type StatInfo struct {
	numblocks  int
	numrecords int
}

func NewStatInfo(numblocks, numrecords int) *StatInfo {
	return &StatInfo{
		numblocks:  numblocks,
		numrecords: numrecords,
	}
}

func (si *StatInfo) BlocksAccessed() int {
	return si.numblocks
}

func (si *StatInfo) RecordsOutput() int {
	return si.numrecords
}

func (si *StatInfo) DistinctValues(fldname string) int {
	// For simplicity, we make an assumption here
	return int(1 + (si.numrecords / 3))
}
