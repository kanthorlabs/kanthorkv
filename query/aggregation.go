package query

import (
	"github.com/kanthorlabs/kanthorkv/record"
)

type AggregationFn interface {
	ProcessFirst(scan record.Scan) error
	ProcessNext(scan record.Scan) error
	FieldName() string
	Value() record.Constant
}
