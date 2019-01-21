package styx

import (
	fmt "fmt"
	"sort"

	badger "github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"
)

// Past is history
type Past struct {
	Slice   []string
	Index   map[string]ReferenceSet
	Order   map[string]int
	Cursors CursorSet
	Indices map[string]map[int]int
}

// Push a dependency into the past
func (past *Past) Push(id string, order int, refs ReferenceSet) {
	if past.Index == nil {
		past.Index = map[string]ReferenceSet{id: refs}
	} else {
		past.Index[id] = refs
	}

	if past.Order == nil {
		past.Order = map[string]int{id: order}
	} else {
		past.Order[id] = order
	}

	past.Slice = append(past.Slice, id)
}

// Sort interface for pastOrder
type pastOrder Past

func (po pastOrder) Len() int { return len(po.Slice) }
func (po pastOrder) Less(a, b int) bool {
	return po.Order[po.Slice[a]] > po.Order[po.Slice[b]]
}

func (po pastOrder) Swap(a, b int) {
	po.Slice[a], po.Slice[b] = po.Slice[b], po.Slice[a]
}

func (past *Past) sortOrder() {
	sort.Stable(pastOrder(*past))
}

// Sort interface for pastCursors
type pastCursors Past

func (pc pastCursors) Len() int { return len(pc.Cursors) }
func (pc pastCursors) Less(a, b int) bool {
	return pc.Cursors[a].Count < pc.Cursors[b].Count
}

func (pc pastCursors) Swap(a, b int) {
	idA, indexA := pc.Cursors[a].ID, pc.Cursors[a].Index
	idB, indexB := pc.Cursors[b].ID, pc.Cursors[b].Index
	pc.Indices[idA][indexA], pc.Indices[idB][indexB] = b, a
	pc.Cursors[a], pc.Cursors[b] = pc.Cursors[b], pc.Cursors[a]
}

func (past *Past) sortCursors() {
	sort.Stable(pastCursors(*past))
}

func (past *Past) setCursors() {
	past.Cursors = CursorSet{}
	for _, refs := range past.Index {
		past.Cursors = append(past.Cursors, refs.toCursorSet()...)
	}
	past.sortCursors()
}

// Dependencies are a slice of indices of assignments
// They're sorted high-to-low
type Dependencies []int

func (deps Dependencies) Len() int           { return len(deps) }
func (deps Dependencies) Less(a, b int) bool { return deps[a] < deps[b] }
func (deps Dependencies) Swap(a, b int)      { deps[a], deps[b] = deps[b], deps[a] }

// An Assignment is a setting of a variable to a value.
type Assignment struct {
	Value        []byte
	ValueRoot    []byte
	Sources      []*Source
	Constraint   ReferenceSet
	Present      ReferenceSet
	Past         *Past
	Future       map[string]ReferenceSet
	Static       CursorSet
	Dependencies Dependencies
}

func (a *Assignment) String() string {
	val := fmt.Sprintln("--- assignment ---")
	val += fmt.Sprintf("Value: %s\n", string(a.Value))
	val += fmt.Sprintf("ValueRoot: %s\n", string(a.ValueRoot))
	val += fmt.Sprintf("Sources: %s\n", sourcesToString(a.Sources))
	val += fmt.Sprintf("Constraint: %s\n", a.Constraint.String())
	val += fmt.Sprintf("Present: %s\n", a.Present.String())
	val += fmt.Sprintln("Future:")
	for id, refs := range a.Future {
		val += fmt.Sprintf("  %s: %s\n", id, refs.String())
	}
	return val
}

// Close all dangling iterators
func (a *Assignment) Close() {
	for _, cursor := range a.Static {
		if cursor.Iterator != nil {
			cursor.Iterator.Close()
			cursor.Iterator = nil
		}
	}
	if a.Past != nil {
		for _, cursor := range a.Past.Cursors {
			if cursor.Iterator != nil {
				cursor.Iterator.Close()
				cursor.Iterator = nil
			}
		}
	}
}

func (a *Assignment) setValueRoot(txn *badger.Txn) {
	fmt.Println("attempting to set the value root")
	fmt.Println(a.String())
	cs := CursorSet{}
	if a.Present.Len() > 0 {
		for _, ref := range a.Present {
			ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
			m := marshalNode("", ref.M)
			n := marshalNode("", ref.N)
			permutation := (ref.Permutation + 1) % 3
			prefix := ValuePrefixes[permutation]
			ref.Cursor.Prefix = assembleKey(prefix, m, n, nil) // ends in \t
			cs = append(cs, ref.Cursor)
		}
	}

	if len(a.Future) > 0 {
		for _, refs := range a.Future {
			for _, ref := range refs {
				ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
				m, n := ld.IsBlankNode(ref.M), ld.IsBlankNode(ref.N)
				if !m && n {
					permutation := (ref.Permutation + 1) % 3
					prefix := MinorPrefixes[permutation]
					ref.Cursor.Prefix = assembleKey(prefix, marshalNode("", ref.M), nil, nil)
				} else if m && !n {
					permutation := (ref.Permutation + 2) % 3
					prefix := MajorPrefixes[permutation]
					ref.Cursor.Prefix = assembleKey(prefix, marshalNode("", ref.N), nil, nil)
				}
				cs = append(cs, ref.Cursor)
			}
		}
	}

	sort.Stable(cs)
	valueRoot := Seek(cs, nil)
	if valueRoot != nil {
		a.ValueRoot = valueRoot
		a.Static = cs
	}
}

// Seek to the next intersection
func (a *Assignment) Seek(value []byte) []byte {
	if value == nil {
		value = a.ValueRoot
	} else {
		value = Seek(a.Static, value)
	}

	if a.Past.Cursors.Len() > 0 {
		for {
			next := Seek(a.Past.Cursors, value)
			if next == nil {
				return nil
			} else if string(next) == string(value) {
				break
			} else {
				value = Seek(a.Static, next)
				if value == nil {
					return nil
				}
			}
		}
	}
	return value
}

// Next value
// ALl the backtracking logic has to happen here
func (a *Assignment) Next() []byte {
	value := Next(a.Static)
	if a.Past.Cursors.Len() > 0 {
		for {
			next := Seek(a.Past.Cursors, value)
			if next == nil {
				return nil
			} else if string(next) == string(value) {
				break
			} else {
				value = Seek(a.Static, next)
				if value == nil {
					return nil
				}
			}
		}
	}
	return value
}

// All *badger.Iterators are initialized during getAssignmentTree. Close them all here!
func closeAssignments(index map[string]*Assignment) {
	for _, assignment := range index {
		if assignment != nil {
			assignment.Close()
		}
	}
}
