package index

import "github.com/kanthorlabs/kanthorkv/record"

type Index interface {
	SearchCost(numblocks, rpb int) int

	// BeforeFirst positions the index before the first record
	// having the specified search key.
	BeforeFirst(searchkey *record.Constant) error

	// Next moves the index to the next record having the search key
	// specified in BeforeFirst. Returns false if there are no more
	// such index records.
	Next() bool

	// GetDataRID returns the RID value stored in the current index record.
	GetDataRID() (*record.RID, error)

	// Insert adds an index record with the specified dataval and datarid values.
	Insert(dataval *record.Constant, datarid *record.RID) error

	// Delete removes the index record with the specified dataval and datarid values.
	Delete(dataval *record.Constant, datarid *record.RID) error

	// Close closes the index.
	Close() error
}
