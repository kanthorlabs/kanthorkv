package buffer

import (
	"os"
	"sync"
	"testing"

	"github.com/jaswdr/faker/v2"
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/log"
	"github.com/stretchr/testify/require"
)

const (
	testBlockSize  = 400
	testBufferSize = 10
	testLogFile    = "testlog"
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

func testdir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "kanthorkv-test-")
	require.NoError(t, err)
	return dir

}

func testdirb(b *testing.B) string {
	dir, err := os.MkdirTemp("", "kanthorkv-test-")
	require.NoError(b, err)
	return dir

}

// setupTest creates a test environment with file manager, log manager and buffer manager
func setupTest(t *testing.T) (file.FileManager, log.LogManager, BufferManager, func()) {
	dir := testdir(t)

	// Create test directory
	err := os.MkdirAll(dir, 0755)
	require.NoError(t, err)

	// Initialize file manager
	fm, err := file.NewFileManager(dir, testBlockSize)
	require.NoError(t, err)

	// Initialize log manager
	lm, err := log.NewLogManager(fm, testLogFile)
	require.NoError(t, err)

	// Initialize buffer manager
	bm, err := NewBufferManager(fm, lm, testBufferSize)
	require.NoError(t, err)

	// Return cleanup function
	cleanup := func() {
		// Remove test directory
		os.RemoveAll(dir)
	}

	return fm, lm, bm, cleanup
}
