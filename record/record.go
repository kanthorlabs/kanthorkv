package record

import (
	"github.com/kanthorlabs/kanthorkv/file"
	"github.com/kanthorlabs/kanthorkv/tx/transaction"
)

type RecordFlag int

const (
	RecordEmpty RecordFlag = iota
	RecordUsed
)

func NewRecordPage(tx transaction.Transaction, blk *file.BlockId, layout *Layout) *RecordPage {
	tx.Pin(blk)
	return &RecordPage{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}
}

// Slot based implementation
type RecordPage struct {
	tx     transaction.Transaction
	blk    *file.BlockId
	layout *Layout
}

func (rp *RecordPage) Block() *file.BlockId {
	return rp.blk
}

func (rp *RecordPage) GetInt(slot int, fldname string) (int, error) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetInt(rp.blk, fldpos)
}

func (rp *RecordPage) SetInt(slot int, fldname string, val int) error {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.SetInt(rp.blk, fldpos, val, true)
}

func (rp *RecordPage) GetString(slot int, fldname string) (string, error) {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.GetString(rp.blk, fldpos)
}

func (rp *RecordPage) SetString(slot int, fldname string, val string) error {
	fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
	return rp.tx.SetString(rp.blk, fldpos, val, true)
}

func (rp *RecordPage) Delete(slot int) error {
	return rp.setFlag(slot, RecordEmpty)
}

func (rp *RecordPage) Format() {
	slot := 0
	for rp.isValidSlot(slot) {
		rp.tx.SetInt(rp.blk, rp.offset(slot), int(RecordEmpty), false)
		for _, fldname := range rp.layout.sch.Fields() {
			fldpos := rp.offset(slot) + rp.layout.Offset(fldname)
			if rp.layout.sch.Type(fldname) == IntegerField {
				rp.tx.SetInt(rp.blk, fldpos, 0, false)
			} else {
				rp.tx.SetString(rp.blk, fldpos, "", false)
			}
		}
		slot++
	}
}

func (rp *RecordPage) NextAfter(slot int) int {
	return rp.SearchAfter(slot, RecordUsed)
}

func (rp *RecordPage) InsertAfter(slot int) int {
	newslot := rp.SearchAfter(slot, RecordUsed)
	if newslot > 0 {
		rp.setFlag(newslot, RecordUsed)
	}
	return newslot
}

func (rp *RecordPage) SearchAfter(slot int, flag RecordFlag) int {
	slot++
	for rp.isValidSlot(slot) {
		slotflag, err := rp.tx.GetInt(rp.blk, rp.offset(slot))
		if err != nil {
			panic(err)
		}

		if slotflag == int(flag) {
			return slot
		}

		slot++
	}
	return -1
}

func (rp *RecordPage) setFlag(slot int, usage RecordFlag) error {
	return rp.tx.SetInt(rp.blk, rp.offset(slot), int(usage), true)
}

func (rp *RecordPage) isValidSlot(slot int) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

func (rp *RecordPage) offset(slot int) int {
	return slot * rp.layout.SlotSize()
}
