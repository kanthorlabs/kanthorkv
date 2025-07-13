package recovery

import (
	"fmt"
	"strings"
)

var basename = "KANTHORKV.TX"

func Errf(err string, args ...string) error {
	return fmt.Errorf("%s.%s: %s", basename, err, strings.Join(args, " | "))
}

func ErrInvalidLogRecord(op int) error {
	args := []string{
		fmt.Sprintf("op=%d", op),
	}
	return Errf("LOG_RECORD.INVALID: ", args...)
}
