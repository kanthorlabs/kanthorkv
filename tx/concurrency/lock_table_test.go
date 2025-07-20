package concurrency

import (
	"strconv"
	"sync"
	"testing"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/stretchr/testify/require"
)

func TestLockTable_SLock(t *testing.T) {
	counter := fk.IntBetween(10, 100)

	t.Run("acquire SLock on different blocks", func(t *testing.T) {
		dir := testdir(t)
		lt := NewLockTable()

		var wg sync.WaitGroup
		for i := range counter {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				blk := file.NewBlockId(dir+"/"+strconv.Itoa(j), 0)
				require.NoError(t, lt.SLock(blk))
			}(i)
		}
	})

	t.Run("acquire SLock on the same block", func(t *testing.T) {
		dir := testdir(t)
		lt := NewLockTable()

		var wg sync.WaitGroup
		blk := file.NewBlockId(dir+"/"+strconv.Itoa(0), 0)

		for range counter {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.NoError(t, lt.SLock(blk))
			}()
		}
		wg.Wait()
	})

	t.Run("acquire SLock on the block that has XLock", func(t *testing.T) {
		dir := testdir(t)
		lt := NewLockTable()

		blk := file.NewBlockId(dir+"/"+strconv.Itoa(0), 0)
		require.NoError(t, lt.XLock(blk))

		var wg sync.WaitGroup
		for i := range counter {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				if i == counter/2 {
					lt.Unlock(blk)
				}
				require.NoError(t, lt.SLock(blk))
			}(i)
		}
		wg.Wait()
	})

	t.Run("acquire SLock on the block that has XLock and TIMEOUT", func(t *testing.T) {
		dir := testdir(t)
		lt := NewLockTable()

		blk := file.NewBlockId(dir+"/"+strconv.Itoa(0), 0)
		require.NoError(t, lt.XLock(blk))

		var wg sync.WaitGroup
		for range counter {
			wg.Add(1)
			go func() {
				defer wg.Done()
				require.ErrorContains(t, lt.SLock(blk), "LOCK.ABORT")
			}()
		}
		wg.Wait()
	})
}

func TestLockTable_XLock(t *testing.T) {
	counter := fk.IntBetween(10, 100)

	t.Run("acquire XLock on different blocks", func(t *testing.T) {
		dir := testdir(t)
		lt := NewLockTable()

		var wg sync.WaitGroup
		for i := range counter {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				blk := file.NewBlockId(dir+"/"+strconv.Itoa(j), 0)
				require.NoError(t, lt.XLock(blk))
			}(i)
		}
		wg.Wait()
	})

	t.Run("acquire XLock on the same block", func(t *testing.T) {
		dir := testdir(t)
		lt := NewLockTable()

		blk := file.NewBlockId(dir+"/"+strconv.Itoa(0), 0)
		require.NoError(t, lt.XLock(blk))
		require.NoError(t, lt.XLock(blk))
	})

	t.Run("acquire XLock on the block that has SLock", func(t *testing.T) {
		dir := testdir(t)
		lt := NewLockTable()

		blk := file.NewBlockId(dir+"/"+strconv.Itoa(0), 0)

		require.NoError(t, lt.SLock(blk))
		// Lock escalation: SLock to XLock
		require.NoError(t, lt.XLock(blk))

		// release all locks
		lt.Unlock(blk)

		// acquire 2 SLocks and then XLock
		require.NoError(t, lt.SLock(blk))
		require.NoError(t, lt.SLock(blk))
		require.ErrorContains(t, lt.XLock(blk), "LOCK.ABORT")
	})
}
