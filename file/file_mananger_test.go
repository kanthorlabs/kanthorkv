package file

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileManager(t *testing.T) {
	dbdir := testdir(t)
	defer os.RemoveAll(dbdir)

	fm, err := NewFileManager(dbdir, BLOCK_SIZE)
	require.NoError(t, err)
	require.NotNil(t, fm)

	filename := fk.RandomStringWithLength(8)
	blk := NewBlockId(filename, 2)

	// first page
	p1 := NewPage(fm.BlockSize())

	pos1 := 88
	str1 := fk.RandomStringWithLength(15)
	p1.SetString(pos1, str1)
	size1 := MaxLength(len(str1))

	pos2 := pos1 + size1
	p1.SetInt(pos2, 345)

	require.NoError(t, fm.Write(blk, p1))

	// second page
	p2 := NewPage(fm.BlockSize())

	require.NoError(t, fm.Read(blk, p2))

	// For these final assertions, we can use assert since no code depends on them
	require.Equal(t, p1.String(pos1), p2.String(pos1))
	require.Equal(t, p1.Int(pos2), p2.Int(pos2))
}

func TestFileManagerAppendAndLength(t *testing.T) {
	dbdir := testdir(t)
	defer os.RemoveAll(dbdir)

	fm, err := NewFileManager(dbdir, BLOCK_SIZE)
	require.NoError(t, err)
	require.NotNil(t, fm)

	// Test with a newly created file
	filename := fk.RandomStringWithLength(8)

	// File length should be 0 initially
	length, err := fm.Length(filename)
	require.NoError(t, err)
	require.Equal(t, 0, length)

	// Append a block and verify length increases
	blk1, err := fm.Append(filename)
	require.NoError(t, err)
	require.Equal(t, 0, blk1.Number())
	require.Equal(t, filename, blk1.Filename())

	length, err = fm.Length(filename)
	require.NoError(t, err)
	require.Equal(t, 1, length)

	// Append another block and verify length again
	blk2, err := fm.Append(filename)
	require.NoError(t, err)
	require.Equal(t, 1, blk2.Number())

	length, err = fm.Length(filename)
	require.NoError(t, err)
	require.Equal(t, 2, length)
}

func TestFileManagerMultipleFiles(t *testing.T) {
	dbdir := testdir(t)
	defer os.RemoveAll(dbdir)

	fm, err := NewFileManager(dbdir, BLOCK_SIZE)
	require.NoError(t, err)
	require.NotNil(t, fm)

	// Create multiple files
	filename1 := fk.RandomStringWithLength(8)
	filename2 := fk.RandomStringWithLength(8)

	// Ensure they're different filenames
	for filename2 == filename1 {
		filename2 = fk.RandomStringWithLength(8)
	}

	// Test operations on first file
	blk1, err := fm.Append(filename1)
	require.NoError(t, err)

	p1 := NewPage(fm.BlockSize())

	pos1 := 100
	val1 := fk.RandomStringWithLength(10)
	p1.SetString(pos1, val1)

	require.NoError(t, fm.Write(blk1, p1))

	// Test operations on second file
	blk2, err := fm.Append(filename2)
	require.NoError(t, err)

	p2 := NewPage(fm.BlockSize())

	pos2 := 200
	val2 := fk.RandomStringWithLength(10)
	p2.SetString(pos2, val2)

	require.NoError(t, fm.Write(blk2, p2))

	// Verify data in both files
	p1Read := NewPage(fm.BlockSize())
	require.NoError(t, fm.Read(blk1, p1Read))
	require.Equal(t, val1, p1Read.String(pos1))

	p2Read := NewPage(fm.BlockSize())
	require.NoError(t, fm.Read(blk2, p2Read))
	require.Equal(t, val2, p2Read.String(pos2))

	// Check lengths are independent
	length1, err := fm.Length(filename1)
	require.NoError(t, err)
	require.Equal(t, 1, length1)

	length2, err := fm.Length(filename2)
	require.NoError(t, err)
	require.Equal(t, 1, length2)
}

func TestFileManagerBlockSize(t *testing.T) {
	dbdir := testdir(t)
	defer os.RemoveAll(dbdir)

	// Test with various block sizes
	blockSizes := []int{400, 800, 1600}

	for _, size := range blockSizes {
		fm, err := NewFileManager(dbdir, size)
		require.NoError(t, err)
		require.Equal(t, size, fm.BlockSize())

		// Test that pages created with this block size work correctly
		p := NewPage(fm.BlockSize())
		require.Equal(t, size, len(p.buffer))
	}
}

func TestFileManagerReadWriteMultiplePositions(t *testing.T) {
	dbdir := testdir(t)
	defer os.RemoveAll(dbdir)

	fm, err := NewFileManager(dbdir, BLOCK_SIZE)
	require.NoError(t, err)

	filename := fk.RandomStringWithLength(8)
	blk, err := fm.Append(filename)
	require.NoError(t, err)

	page := NewPage(fm.BlockSize())

	// Write different data types at different positions
	positions := []struct {
		pos   int
		isInt bool
		val   any
	}{
		{50, true, 12345},
		{100, false, "Hello World"},
		{200, true, 67890},
		{300, false, "Testing multiple positions"},
	}

	for _, p := range positions {
		if p.isInt {
			page.SetInt(p.pos, p.val.(int))
		} else {
			page.SetString(p.pos, p.val.(string))
		}
	}

	// Write the page to disk
	require.NoError(t, fm.Write(blk, page))

	// Read it back
	readPage := NewPage(fm.BlockSize())
	require.NoError(t, fm.Read(blk, readPage))

	// Verify all values
	for _, p := range positions {
		if p.isInt {
			require.Equal(t, p.val.(int), readPage.Int(p.pos))
		} else {
			require.Equal(t, p.val.(string), readPage.String(p.pos))
		}
	}
}
