package record

import (
	"github.com/kanthorlabs/kanthorkv/file"
)

func NewLayout(sch *Schema, offsets map[string]int, slotsize int) *Layout {
	return &Layout{sch, offsets, slotsize}
}

func NewLayoutOfSchema(sch *Schema) *Layout {
	l := &Layout{sch, make(map[string]int), 0}

	pos := file.BLOCK_SIZE
	for _, fldname := range sch.Fields() {
		l.offsets[fldname] = pos
		pos += l.LengthInBytes(fldname)
	}

	return l
}

// Slot based implementation
type Layout struct {
	sch      *Schema
	offsets  map[string]int
	slotsize int
}

func (l *Layout) Schema() *Schema {
	return l.sch
}

func (l *Layout) Offset(fldname string) int {
	return l.offsets[fldname]
}

func (l *Layout) SlotSize() int {
	return l.slotsize
}

func (l *Layout) LengthInBytes(fldname string) int {
	fldtype := l.sch.Type(fldname)
	if fldtype == IntegerField {
		return file.INT_SIZE
	}

	return file.MaxLength(l.sch.Length(fldname))
}
