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

func NewRecord(tx transaction.Transaction, blk *file.BlockId, layout *Layout) *Record {
	tx.Pin(blk)
	return &Record{
		tx:     tx,
		blk:    blk,
		layout: layout,
	}
}

// Slot based implementation
type Record struct {
	tx     transaction.Transaction
	blk    *file.BlockId
	layout *Layout
}

func (r *Record) Block() *file.BlockId {
	return r.blk
}

func (r *Record) GetInt(slot int, fldname string) (int, error) {
	fldpos := r.offset(slot) + r.layout.Offset(fldname)
	return r.tx.GetInt(r.blk, fldpos)
}

func (r *Record) SetInt(slot int, fldname string, val int) error {
	fldpos := r.offset(slot) + r.layout.Offset(fldname)
	return r.tx.SetInt(r.blk, fldpos, val, true)
}

func (r *Record) GetString(slot int, fldname string) (string, error) {
	fldpos := r.offset(slot) + r.layout.Offset(fldname)
	return r.tx.GetString(r.blk, fldpos)
}

func (r *Record) SetString(slot int, fldname string, val string) error {
	fldpos := r.offset(slot) + r.layout.Offset(fldname)
	return r.tx.SetString(r.blk, fldpos, val, true)
}

func (r *Record) Delete(slot int) error {
	return r.setFlag(slot, RecordEmpty)
}

func (r *Record) Format() {
	slot := 0
	for r.isValidSlot(slot) {
		r.tx.SetInt(r.blk, r.offset(slot), int(RecordEmpty), false)
		for _, fldname := range r.layout.sch.Fields() {
			fldpos := r.offset(slot) + r.layout.Offset(fldname)
			if r.layout.sch.Type(fldname) == IntegerField {
				r.tx.SetInt(r.blk, fldpos, 0, false)
			} else {
				r.tx.SetString(r.blk, fldpos, "", false)
			}
		}
		slot++
	}
}

func (r *Record) NextAfter(slot int) int {
	return r.SearchAfter(slot, RecordUsed)
}

func (r *Record) InsertAfter(slot int) int {
	newslot := r.SearchAfter(slot, RecordUsed)
	if newslot > 0 {
		r.setFlag(newslot, RecordUsed)
	}
	return newslot
}

func (r *Record) SearchAfter(slot int, flag RecordFlag) int {
	slot++
	for r.isValidSlot(slot) {
		slotflag, err := r.tx.GetInt(r.blk, r.offset(slot))
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

func (r *Record) setFlag(slot int, usage RecordFlag) error {
	return r.tx.SetInt(r.blk, r.offset(slot), int(usage), true)
}

func (r *Record) isValidSlot(slot int) bool {
	return r.offset(slot+1) <= r.tx.BlockSize()
}

func (r *Record) offset(slot int) int {
	return slot * r.layout.SlotSize()
}
