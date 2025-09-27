package record

import (
	"fmt"
	"hash/fnv"
)

func NewIntConstant(val int) Constant {
	return Constant{ival: &val}
}

func NewStringConstant(val string) Constant {
	return Constant{sval: &val}
}

// Constant represents a value in the database.
type Constant struct {
	ival *int    // using pointer to represent nullable integer
	sval *string // using pointer to represent nullable string
}

// AsInt returns the integer value
func (c Constant) AsInt() int {
	if c.ival == nil {
		panic("Constant does not contain an integer value")
	}
	return *c.ival
}

// AsString returns the string value
func (c Constant) AsString() string {
	if c.sval == nil {
		panic("Constant does not contain a string value")
	}
	return *c.sval
}

func (c Constant) String() string {
	if c.ival != nil {
		return fmt.Sprintf("%d", *c.ival)
	}
	if c.sval != nil {
		return fmt.Sprintf("'%s'", *c.sval)
	}
	panic("Constant does not contain a value")
}

func (c Constant) Equal(other Constant) bool {
	if c.ival != nil && other.ival != nil {
		return *c.ival == *other.ival
	}
	if c.sval != nil && other.sval != nil {
		return *c.sval == *other.sval
	}
	return false
}

// Returns -1 if c < other, 0 if c == other, and 1 if c > other
func (c Constant) Compare(other Constant) int {
	if c.ival != nil && other.ival != nil {
		if *c.ival < *other.ival {
			return -1
		} else if *c.ival > *other.ival {
			return 1
		}
		return 0
	}

	if c.sval != nil && other.sval != nil {
		if *c.sval < *other.sval {
			return -1
		} else if *c.sval > *other.sval {
			return 1
		}
		return 0
	}

	panic("Cannot compare constants of different types")
}

func (c Constant) Hash() int {
	if c.ival != nil {
		return *c.ival
	}

	if c.sval != nil {
		h := fnv.New32a()
		h.Write([]byte(*c.sval))
		return int(h.Sum32())
	}

	panic("Constant does not contain a value")
}
