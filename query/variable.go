package query

import (
	"encoding/binary"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
)

// HasID is either a string representing a variable reference,
// or an Index representing an absolute value from the database
type HasID interface {
	GetID(param interface{}) uint64
}

// A VariableNode is a string with a GetValue method
type VariableNode string

// GetID satisfies the HasID interface for variables by looking up the
// variable's value in the assignmentMap.
func (b VariableNode) GetID(param interface{}) uint64 {
	if g, is := param.(*ConstraintGraph); is {
		id := string(b)
		if i, has := g.Map[id]; has {
			return binary.BigEndian.Uint64(g.Variables[i].Value)
		}
	}
	return 0
}

// ErrInitialCountZero means that the key count of one constraint was zero,
// before intersecting multiple constraints.
var ErrInitialCountZero = fmt.Errorf("Initial constraint count of zero")

// ErrEmptyIntersect means that the *local* intersect of a single variable,
// before joining on any other variables
var ErrEmptyIntersect = fmt.Errorf("Empty intersect")

// ErrInvalidDomain means that provided domain included variables that
// were not in the target graph
var ErrInvalidDomain = fmt.Errorf("Invalid domain")

// ErrInvalidIndex means that provided index included blank nodes or that
// it was too long
var ErrInvalidIndex = fmt.Errorf("Invalid index")

// ErrEmptyJoin means that variables could not be joined
var ErrEmptyJoin = fmt.Errorf("Empty join")

// I is the shorthand type for ids - in this case a []byte slice,
// even though they're always 8 bytes long.
type I = []byte

// Variable is a collection of constraints on a particular blank node.
type Variable struct {
	CS    ConstraintSet // Static and incoming constraints
	D2    ConstraintMap // Outgoing constraints
	Value I             // Tha val
	Root  I             // the first possible value for the variable, without joining on other variables
	Size  int           // The total number of constraints
	Norm  uint64        // The sum of squares of key counts of constraints
}

func (u *Variable) String() (s string) {
	if u.Value != nil {
		s += fmt.Sprintf("Value: %02d\n", binary.BigEndian.Uint64(u.Value))
	}
	if u.Root != nil {
		s += fmt.Sprintf("Root: %02d\n", binary.BigEndian.Uint64(u.Root))
	}
	// s += fmt.Sprintf("DZ: %s\n", u.DZ.String())
	// s += fmt.Sprintf("D1: %s\n", u.D1.String())
	s += fmt.Sprintln("D2:")
	for id, cs := range u.D2 {
		s += fmt.Sprintf("  %d: %s\n", id, cs.String())
	}
	s += fmt.Sprintf("Norm: %d\n", u.Norm)
	s += fmt.Sprintf("Size: %d\n", u.Size)
	return
}

// Close just calls Close on its child constraint sets
func (u *Variable) Close() {
	u.CS.Close()
}

// Sort the constraints by raw count
func (u *Variable) Sort() {
	sort.Stable(u.CS)
}

// Seek to the next intersect value
func (u *Variable) Seek(value I) I {
	return u.CS.Seek(value)
}

// Next returns the next intersect value
func (u *Variable) Next() I {
	return u.CS.Next()
}

// Score cursors, counts, value root, and heuristics
func (u *Variable) Score(txn *badger.Txn) (err error) {
	// u.Norm, u.Size = 0, len(u.D1)+len(u.DZ)
	u.Norm, u.Size = 0, len(u.CS)

	for _, c := range u.CS {
		u.Norm += c.Count * c.Count
	}

	u.Sort()

	if u.Root = u.CS.Seek(u.Root); u.Root == nil {
		err = ErrEmptyIntersect
	}

	return
}

type D []C
type V struct {
	I
	D
}

func (u *Variable) save() *V {
	d := make([]C, 0, u.D2.Len())
	for q, cs := range u.D2 {
		for i, c := range cs {
			d = append(d, c.save(q, i))
		}
	}
	return &V{u.Value, d}
}

func (u *Variable) load(v *V) {
	u.Value = v.I
	for _, d := range v.D {
		c := u.D2[d.i][d.j]
		c.Set(u.Value, d.c)
		c.Seek(u.Value)
	}
}

func (u *Variable) relabel(transformation []int) {
	d2 := make(ConstraintMap, len(u.D2))
	for i, cs := range u.D2 {
		j := transformation[i]
		d2[j] = cs
	}
	u.D2 = d2
}
