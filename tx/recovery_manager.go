package tx

import (
	"github.com/kanthorlabs/kanthorkv/buffer"
)

// RecoveryManager defines the interface for transaction recovery operations
type RecoveryManager interface {
	Commit() error
	Rollback() error
	Recover() error
	SetInt(buff buffer.Buffer, offset int, newval int) (int, error)
	SetString(buff buffer.Buffer, offset int, newval string) (int, error)
}
