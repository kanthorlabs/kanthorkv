package record

// NewRID creates a new RID with the given block number and slot number.
func NewRID(blk int, slot int) RID {
	return RID{Blknum: blk, Slot: slot}
}

// RID is an identifier for a record within a file.
// It consists of a block number and the slot number within that block.
type RID struct {
	Blknum int
	Slot   int
}

func (rid RID) BlockNumber() int {
	return rid.Blknum
}

func (rid RID) Equal(other RID) bool {
	return rid.Blknum == other.Blknum && rid.Slot == other.Slot
}
