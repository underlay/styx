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
	m      []byte     // a convience slot for the []byte of M
	N      HasValue   // The previous (clockwise) element in the triple
	n      []byte     // a convience slot for the []byte of N
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

// Initialize cursors and initial count
func (ref *Reference) Initialize(major bool, txn *badger.Txn) (bool, error) {
	iterator := txn.NewIterator(iteratorOptions)
	ref.Cursor = &Cursor{Iterator: iterator}

	mIndex, mIsIndex := ref.M.(*Index)
	nIndex, nIsIndex := ref.N.(*Index)

	if mIsIndex && nIsIndex {
		// Single reference -> major/minor key
		m, n := ref.m, ref.n

		// Set count
		var key []byte
		if major {
			place := (ref.Place + 1) % 3
			key = assembleKey(MajorPrefixes[place], m, n, nil)
		} else {
			place := (ref.Place + 2) % 3
			key = assembleKey(MinorPrefixes[place], n, m, nil)
		}
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			return !major, fmt.Errorf("Initial reference count of zero: %s", ref.String())
		} else if err != nil {
			return !major, err
		} else if value, err := item.ValueCopy(nil); err != nil {
			return !major, err
		} else {
			ref.Cursor.Count = binary.BigEndian.Uint64(value)
		}

		// Set Cursor Prefix
		place := (ref.Place + 1) % 3
		prefix := TriplePrefixes[place]
		ref.Cursor.Prefix = assembleKey(prefix, m, n, nil)
	} else {
		// Double reference -> index key
		var indexBytes []byte
		var prefix byte

		if mIsIndex && !nIsIndex {
			indexBytes = ref.m
			place := (ref.Place + 1) % 3
			prefix = MinorPrefixes[place]
			ref.Cursor.Count = mIndex.Get(place)
		} else if !mIsIndex && nIsIndex {
			indexBytes = ref.n
			place := (ref.Place + 2) % 3
			prefix = MajorPrefixes[place]
			ref.Cursor.Count = nIndex.Get(place)
		} else {
			return !major, fmt.Errorf("Invalid reference in codex: %s", ref.String())
		}

		// Set Count
		if ref.Cursor.Count == 0 {
			return !major, fmt.Errorf("Initial reference count of zero: %s", ref.String())
		}

		// Set Cursor Prefix
		ref.Cursor.Prefix = assembleKey(prefix, indexBytes, nil, nil)
	}

	return major, nil
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
