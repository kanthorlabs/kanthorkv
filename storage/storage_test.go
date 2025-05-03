package storage

import (
	"os"
	"sync"
	"testing"

	"github.com/jaswdr/faker/v2"
	"github.com/stretchr/testify/require"
)

var (
	fk     faker.Faker
	fkOnce sync.Once
)

func init() {
	fkOnce.Do(func() {
		fk = faker.New()
	})
}

func TestStorage(t *testing.T) {
	dbdir := testdir(t)
	defer os.RemoveAll(dbdir)

	fm, err := NewFileManager(dbdir, 4096)
	require.NoError(t, err)
	require.NotNil(t, fm)

	filename := fk.RandomStringWithLength(8)
	blk, err := NewBlockId(filename, 2)
	require.NoError(t, err)
	require.NotNil(t, blk)

	// first page
	p1, err := NewPage(fm.BlockSize())
	require.NoError(t, err)
	require.NotNil(t, p1)

	pos1 := 88
	str1 := fk.RandomStringWithLength(15)
	p1.SetString(pos1, str1)
	size1 := MaxLength(len(str1))

	pos2 := pos1 + size1
	p1.SetInt(pos2, 345)

	require.NoError(t, fm.Write(blk, p1))

	// second page
	p2, err := NewPage(fm.BlockSize())
	require.NoError(t, err)
	require.NotNil(t, p2)

	require.NoError(t, fm.Read(blk, p2))

	// For these final assertions, we can use assert since no code depends on them
	require.Equal(t, p1.String(pos1), p2.String(pos1))
	require.Equal(t, p1.Int(pos2), p2.Int(pos2))
}

func testdir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "kanthorkv-test-")
	require.NoError(t, err)
	return dir

}
