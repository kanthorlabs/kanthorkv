package buffer

import (
	"fmt"
	"strings"
)

var basename = "KANTHORKV.BUFFER"

func Errf(err string, args ...string) error {
	return fmt.Errorf("%s.%s: %s", basename, err, strings.Join(args, " | "))
}

func ErrBMPinTimeout(block string) error {
	args := []string{
		fmt.Sprintf("block=%s", block),
	}
	return Errf("BUFFER_MANANGER.PIN_TIMEOUT", args...)
}
