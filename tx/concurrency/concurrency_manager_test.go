package concurrency

import (
	"testing"

	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyManager_SLock(t *testing.T) {
	lt := NewLockTable()
	cm := NewConcurrencyManager(lt)

	dir := testdir(t)
	blk := file.NewBlockId(dir+"/0", 0)

	// Obtain shared lock
	require.NoError(t, cm.SLock(blk))

	// non duplicate lock
	require.NoError(t, cm.SLock(blk))
}

func TestConcurrencyManager_XLock(t *testing.T) {
	lt := NewLockTable()
	cm := NewConcurrencyManager(lt)

	dir := testdir(t)

	t.Run("obtain exclusive lock", func(t *testing.T) {
		blk := file.NewBlockId(dir+"/0", 0)

		// Obtain exclusive lock
		require.NoError(t, cm.XLock(blk))

		// non duplicate lock
		require.NoError(t, cm.XLock(blk))

		cm.Release()
	})

	t.Run("XLock is stronger than SLock so can obtain SLock after acquiring a XLock", func(t *testing.T) {
		blk := file.NewBlockId(dir+"/1", 0)

		// Obtain exclusive lock
		require.NoError(t, cm.XLock(blk))

		// non duplicate lock
		require.NoError(t, cm.SLock(blk))

		cm.Release()
	})
}
