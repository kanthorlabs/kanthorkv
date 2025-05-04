package buffer

import (
	"fmt"
	"strings"
)

var basename = "KANTHORKV.BUFFER"

func Errf(err string, args ...string) error {
	return fmt.Errorf("%s.%s: %s", basename, err, strings.Join(args, " | "))
}

func ErrBMPinAbort(blknum int) error {
	args := []string{
		fmt.Sprintf("blknum=%d", blknum),
	}
	return Errf("BUFFER_MANANGER.PIN_ABORT", args...)
}
