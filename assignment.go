package styx

import (
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
	past.Index[id] = refs
	past.Slice = append(past.Slice, id)
	past.Order[id] = order
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
	Present      ReferenceSet
	Constraint   ReferenceSet
	Past         *Past
	Future       map[string]ReferenceSet
	Static       CursorSet
	Trace        [][2][]byte // bet you've never seen this in the wild
	Dependencies Dependencies
}

// func (a *Assignment) String() string {
// 	val := fmt.Sprintln("--- assignment ---")
// 	val += fmt.Sprintf("%v\n", assignment)
// 	return val
// }

// Close the dangling iterators in a.Present
func (a *Assignment) Close() {
	for _, ref := range a.Present {
		ref.Cursor.Iterator.Close()
		ref.Cursor.Iterator = nil
	}
}

func (a *Assignment) setValueRoot(txn *badger.Txn) {
	cs := CursorSet{}
	if a.Present.Len() > 0 {
		for _, ref := range a.Present {
			ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
			m := []byte(ref.M.GetValue())
			n := []byte(ref.N.GetValue())
			permutation := (ref.Permutation + 1) % 3
			prefix := ValuePrefixes[permutation]
			ref.Cursor.Prefix = assembleKey(prefix, m, n, nil) // ends in \t
			cs = append(cs, ref.Cursor)
		}
	}

	if len(a.Future) == 0 {
		for _, refs := range a.Future {
			for _, ref := range refs {
				ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
				m, n := ld.IsBlankNode(ref.M), ld.IsBlankNode(ref.N)
				if !m && n {
					permutation := (ref.Permutation + 1) % 3
					prefix := MinorPrefixes[permutation]
					ref.Cursor.Prefix = assembleKey(prefix, []byte(ref.M.GetValue()), nil, nil)
				} else if m && !n {
					permutation := (ref.Permutation + 2) % 3
					prefix := MajorPrefixes[permutation]
					ref.Cursor.Prefix = assembleKey(prefix, []byte(ref.N.GetValue()), nil, nil)
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
