package concurrency

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

func testdir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "kanthorkv-test-")
	require.NoError(t, err)
	return dir
}
