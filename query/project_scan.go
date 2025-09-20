package query

import (
	"fmt"
	"slices"

	"github.com/kanthorlabs/kanthorkv/record"
)

var _ record.Scan = (*ProjectScan)(nil)

func NewProjectScan(s record.Scan, fields []string) (*ProjectScan, error) {
	return &ProjectScan{s: s, fields: fields}, nil
}

type ProjectScan struct {
	s      record.Scan
	fields []string
}

func (ps *ProjectScan) BeforeFirst() error {
	return ps.s.BeforeFirst()
}

func (ps *ProjectScan) Next() bool {
	return ps.s.Next()
}

func (ps *ProjectScan) GetInt(fldname string) (int, error) {
	if !ps.HasField(fldname) {
		return 0, fmt.Errorf("field %s not found", fldname)
	}
	return ps.s.GetInt(fldname)
}

func (ps *ProjectScan) GetString(fldname string) (string, error) {
	if !ps.HasField(fldname) {
		return "", fmt.Errorf("field %s not found", fldname)
	}
	return ps.s.GetString(fldname)
}

func (ps *ProjectScan) GetVal(fldname string) (record.Constant, error) {
	if !ps.HasField(fldname) {
		return record.Constant{}, fmt.Errorf("field %s not found", fldname)
	}
	return ps.s.GetVal(fldname)
}

func (ps *ProjectScan) Close() error {
	return ps.s.Close()
}

func (ps *ProjectScan) HasField(fldname string) bool {
	return slices.Contains(ps.fields, fldname)
}
