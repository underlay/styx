package query

import (
	"encoding/binary"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger"
)

// HasID is either a string representing a variable reference,
// or an Index representing an absolute value from the database
type HasID interface {
	GetID(param interface{}) uint64
}

// A BlankNode is a string with a GetValue method
type BlankNode string

// GetID satisfies the HasID interface for variables by looking up the
// variable's value in the assignmentMap.
func (b BlankNode) GetID(param interface{}) uint64 {
	if g, is := param.(*ConstraintGraph); is {
		id := string(b)
		if assignment, has := g.Index[id]; has {
			return binary.BigEndian.Uint64(assignment.Value)
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

const pC uint8 = 255 // zoot zoot
const pS uint8 = 0
const pP uint8 = 1
const pO uint8 = 2
const pSP uint8 = 3 // it's important that pSP % 3 == pS, etc
const pPO uint8 = 4
const pOS uint8 = 5
const pSPO uint8 = 9

// Variable is a collection of constraints on a particular blank node.
type Variable struct {
	CS    ConstraintSet
	DZ    ConstraintSet // (almost-always empty) constraints that reference the same variable twice, and one constant
	D1    ConstraintSet // constraints that reference the variable once, and two constants
	D2    ConstraintMap // constraints that reference the variable once, a different variable once, and one constant
	Value []byte        // Tha val
	Root  []byte        // the first possible value for the variable, without joining on other variables
	Size  int           // The total number of constraints
	Norm  uint64        // The sum of squares of key counts of constraints
}

func (v *Variable) String() (s string) {
	s += fmt.Sprintf("DZ: %s\n", v.DZ.String())
	s += fmt.Sprintf("D1: %s\n", v.D1.String())
	s += fmt.Sprintln("D2:")
	for id, cs := range v.D2 {
		s += fmt.Sprintf("  %s: %s\n", id, cs.String())
	}
	s += fmt.Sprintf("Norm: %d\n", v.Norm)
	s += fmt.Sprintf("Size: %d\n", v.Size)
	return
}

// Close just calls Close on its child constraint sets
func (v *Variable) Close() {
	v.DZ.Close()
	v.D1.Close()
	for _, cs := range v.D2 {
		cs.Close()
	}
}

// Sort the constraints by raw count
func (v *Variable) Sort() {
	sort.Stable(v.CS)
}

// Seek to the next intersect value
func (v *Variable) Seek(value []byte) []byte {
	return v.CS.Seek(value)
}

// Next returns the next intersect value
func (v *Variable) Next() []byte {
	return v.CS.Next()
}

// Score cursors, counts, value root, and heuristics
func (v *Variable) Score(txn *badger.Txn) (err error) {
	v.Norm, v.Size = 0, len(v.D1)+len(v.DZ)

	for _, cs := range v.D2 {
		v.Size += len(cs)
	}

	v.CS = make(ConstraintSet, 0, v.Size)

	for _, c := range v.DZ {
		v.Norm += c.Count * c.Count
		v.CS = append(v.CS, c)
	}

	for _, c := range v.D1 {
		v.Norm += c.Count * c.Count
		v.CS = append(v.CS, c)
	}

	for _, cs := range v.D2 {
		for _, c := range cs {
			v.Norm += c.Count * c.Count
			v.CS = append(v.CS, c)
		}
	}

	v.Sort()

	if v.Root = v.CS.Seek(nil); v.Root == nil {
		err = ErrEmptyIntersect
	}

	return
}
