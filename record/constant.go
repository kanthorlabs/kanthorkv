package record

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

func IntConstant(val int) Constant {
	return Constant{ival: &val}
}

func StringConstant(val string) Constant {
	return Constant{sval: &val}
}
