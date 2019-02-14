package main

import (
	"fmt"
)

// A Reference to an occurrence of a variable in a dataset
type Reference struct {
	Graph  string     // The graph in the dataset
	Index  int        // The index of the triple within the graph
	Place  uint8      // The element (subject/predicate/object) within the triple
	M      HasValue   // The next (clockwise) element in the triple
	N      HasValue   // The previous (clockwise) element in the triple
	Cursor *Cursor    // The iteration cursor
	Dual   *Reference // If (M or N) is a blank node, this is a pointer to its reference struct
}

func (ref *Reference) String() string {
	return fmt.Sprintf(
		"%s/%d:%d {%v %v} :: %s",
		ref.Graph, ref.Index, ref.Place,
		ref.M, ref.N,
		ref.Cursor.String(),
	)
}

// Close the reference's cursor's iterator, if it exists
func (ref *Reference) Close() {
	if ref.Cursor != nil {
		if ref.Cursor.Iterator != nil {
			ref.Cursor.Iterator.Close()
			ref.Cursor.Iterator = nil
		}
	}
}

func (ref *Reference) assembleSingleCountKey(assignmentMap AssignmentMap, major bool) ([]byte, bool) {
	var key []byte
	m := ref.M.GetValue(assignmentMap)
	n := ref.N.GetValue(assignmentMap)
	if major {
		place := (ref.Place + 1) % 3
		prefix := MajorPrefixes[place]
		key = assembleKey(prefix, m, n, 0)
	} else {
		place := (ref.Place + 2) % 3
		prefix := MinorPrefixes[place]
		key = assembleKey(prefix, n, m, 0)
	}
	return key, !major
}

// A ReferenceSet is a slice of References.
// It's its own type to stress its order-insignificance, and for ease of future refactoring.
type ReferenceSet []*Reference

// Sort interface for ReferenceSet
func (refs ReferenceSet) Len() int                { return len(refs) }
func (refs ReferenceSet) Swap(a, b int)           { refs[a], refs[b] = refs[b], refs[a] }
func (refs ReferenceSet) Less(a, b int) bool      { return refs[a].Cursor.Count < refs[b].Cursor.Count }
func (refs ReferenceSet) getCursor(i int) *Cursor { return refs[i].Cursor }

func (refs ReferenceSet) toCursorSet() CursorSet {
	cs := CursorSet{}
	for _, ref := range refs {
		cs = append(cs, ref.Cursor)
	}
	return cs
}

func (refs ReferenceSet) String() string {
	s := "[ "
	for i, ref := range refs {
		if i > 0 {
			s += ", "
		}
		s += ref.String()
	}
	return s + " ]"
}

// A ReferenceMap is a map of string variable labels to reference sets.
type ReferenceMap map[string]ReferenceSet
