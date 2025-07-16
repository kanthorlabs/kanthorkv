package concurrency

import (
	"sync"
	"time"

	"github.com/kanthorlabs/kanthorkv/file"
)

const MAX_WAIT_TIME = 10 * time.Second

type LockTable struct {
	mu      sync.Mutex
	locks   map[*file.BlockId]int
	waiters map[*file.BlockId]chan struct{}
}

func NewLockTable() *LockTable {
	return &LockTable{
		locks:   make(map[*file.BlockId]int),
		waiters: make(map[*file.BlockId]chan struct{}),
	}
}

func (lt *LockTable) SLock(blk *file.BlockId) error {
	lt.mu.Lock()

	start := time.Now()
	for lt.locks[blk] == -1 {
		ch := lt.channel(blk)
		// Unlock the mutex so that other transactions can join the waitlist
		lt.mu.Unlock()

		if time.Since(start) > MAX_WAIT_TIME {
			return ErrLockAbort(blk)
		}

		// Wait on the channel with a timeout
		select {
		case <-ch:
			lt.mu.Lock()
		case <-time.After(MAX_WAIT_TIME):
			return ErrLockAbort(blk)
		}
	}

	val := lt.locks[blk] // will not be negative
	lt.locks[blk] = val + 1
	lt.mu.Unlock()
	return nil
}

func (lt *LockTable) XLock(blk *file.BlockId) error {
	lt.mu.Lock()

	start := time.Now()
	for lt.locks[blk] != 0 {
		ch := lt.channel(blk)
		// Unlock the mutex so that other transactions can join the waitlist
		lt.mu.Unlock()

		if time.Since(start) > MAX_WAIT_TIME {
			return ErrLockAbort(blk)
		}

		// Wait on the channel with a timeout
		select {
		case <-ch:
			lt.mu.Lock()
		case <-time.After(MAX_WAIT_TIME):
			return ErrLockAbort(blk)
		}
	}

	lt.locks[blk] = -1
	lt.mu.Unlock()
	return nil
}

func (lt *LockTable) Unlock(blk *file.BlockId) error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	if val, exists := lt.locks[blk]; exists {
		if val > 1 {
			lt.locks[blk] = val - 1
		} else {
			delete(lt.locks, blk)
		}
	}

	// Signal all goroutines waiting for this block (and remove the channel)
	if ch, exists := lt.waiters[blk]; exists {
		close(ch)
		delete(lt.waiters, blk)
	}
	return nil
}

func (lt *LockTable) channel(blk *file.BlockId) chan struct{} {
	if ch, exists := lt.waiters[blk]; exists {
		return ch
	}

	ch := make(chan struct{})
	lt.waiters[blk] = ch
	return ch
}
