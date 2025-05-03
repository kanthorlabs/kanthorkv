package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLength(t *testing.T) {
	dbdir := testdir(t)
	defer os.RemoveAll(dbdir)

	fm, err := NewFileManager(dbdir, 4096)
	require.NoError(t, err)
	require.NotNil(t, fm)

	filename := fk.RandomStringWithLength(8)
	size, err := fm.Length(filename)
	require.NoError(t, err)
	require.Equal(t, int64(0), size)
}
