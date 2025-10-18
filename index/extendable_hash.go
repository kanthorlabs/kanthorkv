package index

import (
	"fmt"

	"github.com/kanthorlabs/kanthorkv/record"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

var _ Index = (*ExtendableHashIndex)(nil)

// NewExtendableHashIndex creates a new extendable hash index with an initial global depth of 1.
func NewExtendableHashIndex(tx transaction.Transaction, idxName string, idxLayout *record.Layout) (Index, error) {
	// Start with global depth of 1 (2 directory entries)
	globalDepth := 1
	dirSize := 1 << globalDepth // 2^globalDepth

	// Initialize directory with bucket pointers
	directory := make([]int, dirSize)
	localDepths := make([]int, dirSize)

	// Initially, all directory entries point to bucket 0 with local depth 1
	for i := 0; i < dirSize; i++ {
		directory[i] = 0
		localDepths[i] = 1
	}

	return &ExtendableHashIndex{
		tx:          tx,
		idxName:     idxName,
		idxLayout:   idxLayout,
		globalDepth: globalDepth,
		directory:   directory,
		localDepths: localDepths,
	}, nil
}

// ExtendableHashIndex implements an extendable hash index with dynamic directory growth.
type ExtendableHashIndex struct {
	tx          transaction.Transaction
	idxName     string
	idxLayout   *record.Layout
	searchKey   *record.Constant
	ts          *record.TableScan
	globalDepth int   // number of bits used for directory indexing
	directory   []int // maps hash values to bucket numbers
	localDepths []int // local depth for each directory entry
}

// SearchCost returns the estimated cost of searching for a record.
func (ehi *ExtendableHashIndex) SearchCost(numblocks, rpb int) int {
	// Extendable hashing typically requires 2 disk accesses:
	// 1 for the directory and 1 for the bucket
	return 2
}

// BeforeFirst positions the index before the first record having the specified search key.
func (ehi *ExtendableHashIndex) BeforeFirst(searchkey *record.Constant) error {
	if err := ehi.Close(); err != nil {
		return err
	}

	ehi.searchKey = searchkey
	bucket := ehi.getBucket(searchkey.Hash())
	tblname := fmt.Sprintf("%s%d", ehi.idxName, bucket)
	ts, err := record.NewTableScan(ehi.tx, tblname, ehi.idxLayout)
	if err != nil {
		return err
	}
	ehi.ts = ts
	return nil
}

// Next moves the index to the next record having the search key specified in BeforeFirst.
func (ehi *ExtendableHashIndex) Next() bool {
	if ehi.ts == nil {
		return false
	}
	for ehi.ts.Next() {
		dataval, err := ehi.ts.GetVal("dataval")
		if err != nil {
			panic(err)
		}
		if dataval.Equal(*ehi.searchKey) {
			return true
		}
	}
	return false
}

// GetDataRID returns the RID value stored in the current index record.
func (ehi *ExtendableHashIndex) GetDataRID() (*record.RID, error) {
	blknum, err := ehi.ts.GetInt("block")
	if err != nil {
		return nil, err
	}
	slot, err := ehi.ts.GetInt("id")
	if err != nil {
		return nil, err
	}
	return &record.RID{Blknum: int(blknum), Slot: int(slot)}, nil
}

// Insert adds an index record with the specified dataval and datarid values.
// Implements the extendable hashing algorithm:
// 1. Hash the record's dataval to get bucket b.
// 2. Find B = Dir[b]. Let L be the local depth of block B.
// 3a. If the record fits into B, insert it and return.
// 3b. If the record does not fit in B:
//   - Allocate a new block B' in the bucket file.
//   - Set the local depth of both B and B' to be L+1.
//   - Adjust the bucket directory so that all buckets having the rightmost L+1 bits
//     1b_L...b_2b_1 will point to B'.
//   - Re-insert each record from B into the index.
//   - Try again to insert the new record into the index.
func (ehi *ExtendableHashIndex) Insert(dataval *record.Constant, datarid *record.RID) error {
	return ehi.insertWithRetry(dataval, datarid, 0)
}

// insertWithRetry handles the recursive insertion logic with bucket splitting.
func (ehi *ExtendableHashIndex) insertWithRetry(dataval *record.Constant, datarid *record.RID, depth int) error {
	// Prevent infinite recursion
	const maxDepth = 10
	if depth > maxDepth {
		return fmt.Errorf("max recursion depth exceeded during insert")
	}

	// Step 1: Hash the record's dataval to get bucket b
	hashVal := dataval.Hash()
	dirIndex := ehi.getDirIndex(hashVal)
	bucketNum := ehi.directory[dirIndex]
	localDepth := ehi.localDepths[dirIndex]

	// Open the bucket's table
	tblname := fmt.Sprintf("%s%d", ehi.idxName, bucketNum)
	ts, err := record.NewTableScan(ehi.tx, tblname, ehi.idxLayout)
	if err != nil {
		return err
	}
	defer ts.Close()

	// Step 3a: Try to insert into bucket B
	if err := ts.Insert(); err == nil {
		// Record fits, insert and return
		if err := ts.SetInt("block", datarid.Blknum); err != nil {
			return err
		}
		if err := ts.SetInt("id", datarid.Slot); err != nil {
			return err
		}
		if err := ts.SetVal("dataval", *dataval); err != nil {
			return err
		}
		return nil
	}

	// Step 3b: Record does not fit, need to split the bucket
	return ehi.splitAndInsert(dirIndex, bucketNum, localDepth, dataval, datarid, depth)
}

// splitAndInsert splits a full bucket and redistributes records.
func (ehi *ExtendableHashIndex) splitAndInsert(dirIndex, bucketNum, localDepth int, dataval *record.Constant, datarid *record.RID, depth int) error {
	// Check if we need to double the directory
	if localDepth == ehi.globalDepth {
		ehi.doubleDirectory()
	}

	// Allocate a new bucket B'
	newBucketNum := ehi.allocateNewBucket()

	// Set the local depth of both B and B' to L+1
	newLocalDepth := localDepth + 1

	// Update directory entries
	ehi.updateDirectoryAfterSplit(bucketNum, newBucketNum, newLocalDepth)

	// Re-insert all records from the old bucket
	if err := ehi.redistributeRecords(bucketNum); err != nil {
		return err
	}

	// Try again to insert the new record
	return ehi.insertWithRetry(dataval, datarid, depth+1)
}

// doubleDirectory doubles the size of the directory when global depth needs to increase.
func (ehi *ExtendableHashIndex) doubleDirectory() {
	ehi.globalDepth++
	oldSize := len(ehi.directory)
	newSize := 1 << ehi.globalDepth

	newDirectory := make([]int, newSize)
	newLocalDepths := make([]int, newSize)

	// Copy existing entries and duplicate them
	for i := range oldSize {
		newDirectory[i] = ehi.directory[i]
		newDirectory[i+oldSize] = ehi.directory[i]
		newLocalDepths[i] = ehi.localDepths[i]
		newLocalDepths[i+oldSize] = ehi.localDepths[i]
	}

	ehi.directory = newDirectory
	ehi.localDepths = newLocalDepths
}

// allocateNewBucket returns a new unique bucket number.
func (ehi *ExtendableHashIndex) allocateNewBucket() int {
	// Find the maximum bucket number and add 1
	maxBucket := 0
	for _, bucket := range ehi.directory {
		if bucket > maxBucket {
			maxBucket = bucket
		}
	}
	return maxBucket + 1
}

// updateDirectoryAfterSplit updates directory entries to point to the new bucket.
func (ehi *ExtendableHashIndex) updateDirectoryAfterSplit(oldBucket, newBucket, newLocalDepth int) {
	// Get the hash pattern for this directory index
	mask := (1 << newLocalDepth) - 1

	// Update all directory entries that should point to the new bucket
	for i := 0; i < len(ehi.directory); i++ {
		if ehi.directory[i] == oldBucket {
			// Check if the rightmost newLocalDepth bits match the pattern
			// that should go to the new bucket
			if (i & mask) >= (1 << (newLocalDepth - 1)) {
				ehi.directory[i] = newBucket
			}
			ehi.localDepths[i] = newLocalDepth
		}
	}
}

// redistributeRecords re-inserts all records from a bucket into the index.
func (ehi *ExtendableHashIndex) redistributeRecords(bucketNum int) error {
	tblname := fmt.Sprintf("%s%d", ehi.idxName, bucketNum)
	ts, err := record.NewTableScan(ehi.tx, tblname, ehi.idxLayout)
	if err != nil {
		return err
	}
	defer ts.Close()

	// Collect all records first to avoid concurrent modification
	type recordEntry struct {
		dataval record.Constant
		rid     record.RID
	}
	var records []recordEntry

	ts.BeforeFirst()
	for ts.Next() {
		dataval, err := ts.GetVal("dataval")
		if err != nil {
			return err
		}
		rid, err := ehi.GetDataRID()
		if err != nil {
			return err
		}
		records = append(records, recordEntry{dataval: dataval, rid: *rid})
	}

	// Clear the old bucket by deleting all records
	ts.BeforeFirst()
	for ts.Next() {
		if err := ts.Delete(); err != nil {
			return err
		}
	}

	// Re-insert records into appropriate buckets
	for _, rec := range records {
		newBucketNum := ehi.getBucket(rec.dataval.Hash())
		if newBucketNum == bucketNum {
			// Record stays in the same bucket, insert directly
			if err := ts.Insert(); err != nil {
				return err
			}
			if err := ts.SetInt("block", rec.rid.Blknum); err != nil {
				return err
			}
			if err := ts.SetInt("id", rec.rid.Slot); err != nil {
				return err
			}
			if err := ts.SetVal("dataval", rec.dataval); err != nil {
				return err
			}
		} else {
			// Record goes to a different bucket
			newTblname := fmt.Sprintf("%s%d", ehi.idxName, newBucketNum)
			newTs, err := record.NewTableScan(ehi.tx, newTblname, ehi.idxLayout)
			if err != nil {
				return err
			}
			if err := newTs.Insert(); err != nil {
				newTs.Close()
				return err
			}
			if err := newTs.SetInt("block", rec.rid.Blknum); err != nil {
				newTs.Close()
				return err
			}
			if err := newTs.SetInt("id", rec.rid.Slot); err != nil {
				newTs.Close()
				return err
			}
			if err := newTs.SetVal("dataval", rec.dataval); err != nil {
				newTs.Close()
				return err
			}
			newTs.Close()
		}
	}

	return nil
}

// Delete removes the index record with the specified dataval and datarid values.
func (ehi *ExtendableHashIndex) Delete(dataval *record.Constant, datarid *record.RID) error {
	if err := ehi.BeforeFirst(dataval); err != nil {
		return err
	}

	for ehi.Next() {
		currRID, err := ehi.GetDataRID()
		if err != nil {
			return err
		}
		if currRID.Equal(*datarid) {
			return ehi.ts.Delete()
		}
	}
	return nil
}

// Close closes the index.
func (ehi *ExtendableHashIndex) Close() error {
	if ehi.ts != nil {
		err := ehi.ts.Close()
		ehi.ts = nil
		return err
	}
	return nil
}

// getDirIndex returns the directory index for a given hash value.
func (ehi *ExtendableHashIndex) getDirIndex(hashVal int) int {
	// Use the rightmost globalDepth bits of the hash
	mask := (1 << ehi.globalDepth) - 1
	return hashVal & mask
}

// getBucket returns the bucket number for a given hash value.
func (ehi *ExtendableHashIndex) getBucket(hashVal int) int {
	dirIndex := ehi.getDirIndex(hashVal)
	return ehi.directory[dirIndex]
}
