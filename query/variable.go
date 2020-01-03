package query

import (
	"encoding/binary"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
)

// Node is either a string representing a variable reference,
// or an Index representing an absolute value from the database
type Node interface {
	ID(param interface{}) uint64
}

// A VariableNode is a string with a GetValue method
type VariableNode string

// ID satisfies the Node interface for variables by looking up the
// variable's value in the assignmentMap.
func (b VariableNode) ID(param interface{}) uint64 {
	if g, is := param.(*cursorGraph); is {
		id := string(b)
		if i, has := g.ids[id]; has {
			return binary.BigEndian.Uint64(g.variables[i].value)
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

// variable is a collection of constraints on a particular blank node.
type variable struct {
	cs    constraintSet // Static and incoming constraints
	d2    constraintMap // Outgoing constraints
	value I             // Tha val
	root  I             // the first possible value for the variable, without joining on other variables
	size  int           // The total number of constraints
	norm  uint64        // The sum of squares of key counts of constraints
}

func (u *variable) String() (s string) {
	if u.value != nil {
		s += fmt.Sprintf("Value: %02d\n", binary.BigEndian.Uint64(u.value))
	}
	if u.root != nil {
		s += fmt.Sprintf("Root: %02d\n", binary.BigEndian.Uint64(u.root))
	}
	// s += fmt.Sprintf("DZ: %s\n", u.DZ.String())
	// s += fmt.Sprintf("D1: %s\n", u.D1.String())
	s += fmt.Sprintln("D2:")
	for id, cs := range u.d2 {
		s += fmt.Sprintf("  %d: %s\n", id, cs.String())
	}
	s += fmt.Sprintf("Norm: %d\n", u.norm)
	s += fmt.Sprintf("Size: %d\n", u.size)
	return
}

// Close just calls Close on its child constraint sets
func (u *variable) Close() {
	if u != nil {
		u.cs.Close()
	}
}

// Sort the constraints by raw count
func (u *variable) Sort() {
	sort.Stable(u.cs)
}

// Seek to the next intersect value
func (u *variable) Seek(value I) I {
	return u.cs.Seek(value)
}

// Next returns the next intersect value
func (u *variable) Next() I {
	return u.cs.Next()
}

// Score cursors, counts, value root, and heuristics
func (u *variable) Score(txn *badger.Txn) (err error) {
	// u.Norm, u.Size = 0, len(u.D1)+len(u.DZ)
	u.norm, u.size = 0, u.cs.Len()

	for _, c := range u.cs {
		u.norm += c.count * c.count
	}

	u.Sort()

	// It's important to u.cs.Seek(u.root) instead
	// of u.cs.Seek(nil) because u.root might have been
	// set already with a provided index.
	if u.root = u.cs.Seek(u.root); u.root == nil {
		err = ErrEmptyIntersect
	}

	return
}

// caches is a slice of C structs
type caches []cache

// V is a struct that caches a variable's total state
type V struct {
	I
	caches
}

func (u *variable) save() *V {
	d := make(caches, 0, u.d2.Len())
	for q, cs := range u.d2 {
		for i, c := range cs {
			d = append(d, c.save(q, i))
		}
	}
	return &V{u.value, d}
}

func (u *variable) load(v *V) {
	u.value = v.I
	for _, d := range v.caches {
		c := u.d2[d.i][d.j]
		c.Set(u.value, d.c)
		c.Seek(u.value)
	}
}

// func (u *variable) relabel(transformation []int) {
// 	d2 := make(constraintMap, len(u.d2))
// 	for i, cs := range u.d2 {
// 		j := transformation[i]
// 		d2[j] = cs
// 	}
// 	u.d2 = d2
// }
