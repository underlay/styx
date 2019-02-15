package main

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger"
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

// Initialize populates cursors and initial count
func (ref *Reference) Initialize(major bool, txn *badger.Txn) (bool, error) {
	var err error
	var count uint64
	count, major, err = ref.getCount(nil, major, txn)
	if err != nil {
		return major, err
	} else if count == 0 {
		return major, fmt.Errorf("Initial reference count of zero: %s", ref.String())
	}

	ref.Cursor = &Cursor{Count: count}
	return major, nil
}

func (ref *Reference) getCount(assignmentMap *AssignmentMap, major bool, txn *badger.Txn) (uint64, bool, error) {
	mIndex, mIsIndex := ref.M.(*Index)
	nIndex, nIsIndex := ref.N.(*Index)
	m, n := ref.M.GetValue(assignmentMap), ref.N.GetValue(assignmentMap)

	if m > 0 && n > 0 {
		// Single reference -> major/minor key
		var key []byte
		if major {
			place := (ref.Place + 1) % 3
			key = assembleKey(MajorPrefixes[place], m, n, 0)
		} else {
			place := (ref.Place + 2) % 3
			key = assembleKey(MinorPrefixes[place], n, m, 0)
		}
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return 0, !major, nil
		} else if err != nil {
			return 0, !major, err
		} else if count, err := item.ValueCopy(nil); err != nil {
			return 0, !major, err
		} else {
			return binary.BigEndian.Uint64(count), !major, nil
		}
	}

	// Double reference -> index key
	var place uint8
	var index *Index
	if mIsIndex && !nIsIndex {
		index = mIndex
		place = (ref.Place + 1) % 3
	} else if !mIsIndex && nIsIndex {
		index = nIndex
		place = (ref.Place + 2) % 3
	} else {
		return 0, false, fmt.Errorf("Invalid reference in codex: %s", ref.String())
	}

	return index.Get(place), !major, nil
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
