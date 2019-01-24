package styx

import (
	"fmt"
	"sort"
	"strings"

	badger "github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"
)

// Past is history
type Past struct {
	Slice   []string
	Index   map[string]referenceSet
	Order   map[string]int
	Cursors CursorSet
	Indices map[string]map[int]int
}

func (past *Past) String() string {
	val := fmt.Sprintf("Past: %s\n", past.Cursors.String())
	for _, id := range past.Slice {
		val += fmt.Sprintf("  %s: %s\n", id, past.Index[id].String())
		pad := strings.Repeat(" ", len(id))
		for _, ref := range past.Index[id] {
			if ref.Cursor != nil {
				val += fmt.Sprintf("  %s  %s | %d\n", pad, string(ref.Cursor.Prefix), ref.Cursor.Count)
			}
		}
	}
	return val
}

// Push a dependency into the past
func (past *Past) Push(dep string, order int, refs referenceSet) {
	if past.Index == nil {
		past.Index = map[string]referenceSet{dep: refs}
	} else {
		past.Index[dep] = refs
	}

	if past.Order == nil {
		past.Order = map[string]int{dep: order}
	} else {
		past.Order[dep] = order
	}

	past.Slice = append(past.Slice, dep)
}

func (past *Past) insertIndex(id string, index int, value int) {
	if past.Indices == nil {
		past.Indices = map[string]map[int]int{id: map[int]int{index: value}}
	} else if past.Indices[id] == nil {
		past.Indices[id] = map[int]int{index: value}
	} else {
		past.Indices[id][index] = value
	}
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
	Constraint   referenceSet
	Present      referenceSet
	Past         *Past
	Future       map[string]referenceSet
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
	if a.Past != nil {
		val += a.Past.String()
	}
	return val
}

// Close all dangling iterators
// func (a *Assignment) Close() {
// 	for _, cursor := range a.Static {
// 		if cursor.Iterator != nil {
// 			cursor.Iterator.Close()
// 			cursor.Iterator = nil
// 		}
// 	}
// 	if a.Past != nil {
// 		for _, cursor := range a.Past.Cursors {
// 			if cursor.Iterator != nil {
// 				cursor.Iterator.Close()
// 				cursor.Iterator = nil
// 			}
// 		}
// 	}
// }

func (a *Assignment) setValueRoot(txn *badger.Txn) {
	// fmt.Println("attempting to set the value root")
	// fmt.Println(a.String())
	cs := CursorSet{}
	if a.Present.Len() > 0 {
		for _, ref := range a.Present {
			ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
			m := marshalNode("", ref.M)
			n := marshalNode("", ref.N)
			permutation := (ref.Permutation + 1) % 3
			prefix := TriplePrefixes[permutation]
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

	length := len(cs)
	sort.Stable(cs)

	if len(a.Past.Slice) > 0 {
		for _, id := range a.Past.Slice {
			for _, ref := range a.Past.Index[id] {
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

	valueRoot := Seek(cs, nil)
	a.Static = cs[:length]
	if valueRoot != nil {
		a.ValueRoot = valueRoot
	}
}

// Seek to the next intersection
func (a *Assignment) Seek(value []byte) []byte {
	if value == nil {
		value = a.ValueRoot
	} else {
		value = Seek(a.Static, value)
	}

	fmt.Println("starting to seek from", string(value), a.Past.Slice)
	fmt.Println("past cursor set", a.Past.Cursors)
	if a.Past.Cursors.Len() > 0 {
		for {
			fmt.Println("top of loop with value", string(value))
			next := Seek(a.Past.Cursors, value)
			fmt.Println("got next value of", string(next))
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
// func closeAssignments(index map[string]*Assignment) {
// 	for _, assignment := range index {
// 		if assignment != nil {
// 			assignment.Close()
// 		}
// 	}
// }
