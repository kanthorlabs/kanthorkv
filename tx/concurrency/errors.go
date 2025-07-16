package concurrency

import (
	"fmt"
	"strings"

	"github.com/kanthorlabs/kanthorkv/file"
)

var basename = "KANTHORKV.TX.CONCURRENCY"

func Errf(err string, args ...string) error {
	return fmt.Errorf("%s.%s: %s", basename, err, strings.Join(args, " | "))
}

func ErrLockAbort(blk *file.BlockId) error {
	args := []string{
		fmt.Sprintf("blk=%s", blk.String()),
	}
	return Errf("LOCK.ABORT: ", args...)
}
