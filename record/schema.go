package record

// NewSchema creates a new Schema instance.
func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

type Schema struct {
	fields []string
	info   map[string]FieldInfo
}

func (s *Schema) AddField(fldname string, t FieldType, length int) {
	s.fields = append(s.fields, fldname)
	s.info[fldname] = FieldInfo{t, length}
}

func (s *Schema) AddIntField(fldname string) {
	// The length of a field is only meaningful for string fields
	s.AddField(fldname, IntegerField, 0)
}

func (s *Schema) AddStringField(fldname string, length int) {
	s.AddField(fldname, StringField, length)
}

func (s *Schema) Add(fildname string, sch *Schema) {
	t := sch.Type(fildname)
	l := sch.Length(fildname)
	s.AddField(fildname, t, l)
}

func (s *Schema) AddAll(sch *Schema) {
	for _, fldname := range sch.Fields() {
		s.Add(fldname, sch)
	}
}

func (s *Schema) Type(fldname string) FieldType {
	return s.info[fldname].t
}

func (s *Schema) Length(fldname string) int {
	return s.info[fldname].l
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) HasField(fldname string) bool {
	_, exists := s.info[fldname]
	return exists
}

type FieldInfo struct {
	t FieldType
	l int
}

type FieldType int

const (
	IntegerField FieldType = 1
	StringField  FieldType = 2
)

// String implements the Stringer interface for FieldType
func (t FieldType) String() string {
	switch t {
	case IntegerField:
		return "INT"
	case StringField:
		return "VARCHAR"
	default:
		return "UNKNOWN"
	}
}
