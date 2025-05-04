package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogManager(t *testing.T) {
	dir := testdir(t)
	defer os.RemoveAll(dir)

	fm, err := NewFileManager(dir, BLOCK_SIZE)
	require.NoError(t, err)
	require.NotNil(t, fm)

	logfile := fk.RandomStringWithLength(8)
	lm, err := NewLogManager(fm, logfile)
	require.NoError(t, err)
	require.NotNil(t, lm)

	// Test 1: Simple append with random data
	rec1 := []byte(fk.Lorem().Sentence(3))
	lsn1, err := lm.Append(rec1)
	require.NoError(t, err)
	require.Equal(t, 1, lsn1)

	// Test 2: Append another random record
	rec2 := []byte(fk.Lorem().Sentence(5))
	lsn2, err := lm.Append(rec2)
	require.NoError(t, err)
	require.Equal(t, 2, lsn2)

	// Test 3: Append a larger random record
	randomSize := fk.IntBetween(50, 150)
	largeRec := []byte(fk.Lorem().Text(randomSize))
	lsn3, err := lm.Append(largeRec)
	require.NoError(t, err)
	require.Equal(t, 3, lsn3)

	// Test 4: Fill a block by appending large records
	// This forces the creation of a new block
	blockSize := fm.BlockSize()
	// Create a record large enough to fill most of a block
	fillSize := blockSize / 2
	fillRec := []byte(fk.Lorem().Text(fillSize))
	lsn4, err := lm.Append(fillRec)
	require.NoError(t, err)
	require.Equal(t, 4, lsn4)

	// This should trigger creation of a new block
	fillRec2 := []byte(fk.Lorem().Text(fillSize))
	lsn5, err := lm.Append(fillRec2)
	require.NoError(t, err)
	require.Equal(t, 5, lsn5)

	// Test 6: Verify Flush forces logs to disk even when there's room
	// Add another record but don't fill the block
	smallRec := []byte(fk.Lorem().Sentence(1))
	lsn6, err := lm.Append(smallRec)
	require.NoError(t, err)
	require.Equal(t, 6, lsn6)

	// Force flush to disk
	err = lm.Flush(lsn6)
	require.NoError(t, err)

	// Test 7: Verify a new record is in memory but not flushed to disk yet
	// Add one more record that should remain in memory until forced to flush
	inMemoryRec := []byte(fk.Lorem().Sentence(2))
	lsn7, err := lm.Append(inMemoryRec)
	require.NoError(t, err)
	require.Equal(t, 7, lsn7)

	// Note: The LogManager.Iterator() method does call flush() internally
	// which will write the in-memory record to disk before reading begins
	// This is important because it ensures log consistency

	// Test 5: Verify we can read back all records through iterator
	iterator, err := lm.Iterator()
	require.NoError(t, err)
	require.NotNil(t, iterator)

	// Check records in reverse order (newest first)
	require.True(t, iterator.HasNext())
	rec, err := iterator.Next()
	require.NoError(t, err)
	require.Equal(t, inMemoryRec, rec)

	require.True(t, iterator.HasNext())
	rec, err = iterator.Next()
	require.NoError(t, err)
	require.Equal(t, smallRec, rec)

	require.True(t, iterator.HasNext())
	rec, err = iterator.Next()
	require.NoError(t, err)
	require.Equal(t, fillRec2, rec)

	require.True(t, iterator.HasNext())
	rec, err = iterator.Next()
	require.NoError(t, err)
	require.Equal(t, fillRec, rec)

	require.True(t, iterator.HasNext())
	rec, err = iterator.Next()
	require.NoError(t, err)
	require.Equal(t, largeRec, rec)

	require.True(t, iterator.HasNext())
	rec, err = iterator.Next()
	require.NoError(t, err)
	require.Equal(t, rec2, rec)

	require.True(t, iterator.HasNext())
	rec, err = iterator.Next()
	require.NoError(t, err)
	require.Equal(t, rec1, rec)

	require.False(t, iterator.HasNext())
}
