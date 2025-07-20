package concurrency

import (
	"github.com/kanthorlabs/kanthorkv/file"
)

type LockType int

const (
	ExclusiveLock LockType = -1
	SharedLock    LockType = 1
)

func NewConcurrencyManager(lt *LockTable) *ConcurrencyManager {
	return &ConcurrencyManager{
		lt:    lt,
		locks: make(map[*file.BlockId]LockType),
	}
}

type ConcurrencyManager struct {
	lt    *LockTable
	locks map[*file.BlockId]LockType
}

func (cm *ConcurrencyManager) SLock(blk *file.BlockId) error {
	if _, exist := cm.locks[blk]; exist {
		return nil
	}

	if err := cm.lt.SLock(blk); err != nil {
		return err
	}
	cm.locks[blk] = SharedLock
	return nil
}

func (cm *ConcurrencyManager) XLock(blk *file.BlockId) error {
	if lock, exist := cm.locks[blk]; exist && lock == ExclusiveLock {
		return nil
	}

	// Obtain shared lock first
	if err := cm.lt.SLock(blk); err != nil {
		return err
	}
	// Upgrade to exclusive lock
	if err := cm.lt.XLock(blk); err != nil {
		return err
	}
	cm.locks[blk] = ExclusiveLock
	return nil
}

func (cm *ConcurrencyManager) Release() {
	for blk := range cm.locks {
		cm.lt.Unlock(blk)
	}
}
