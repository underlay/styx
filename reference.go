package styx

import (
	fmt "fmt"

	ld "github.com/piprate/json-gold/ld"
)

// A Reference to an occurrence of a variable in a dataset
type Reference struct {
	Graph       string     // The graph in the dataset
	Index       int        // The index of the triple within the graph
	Permutation uint8      // The element (subject/predicate/object) within the triple
	M           ld.Node    // The next (clockwise) element in the triple
	N           ld.Node    // The previous (clockwise) element in the triple
	Cursor      *Cursor    // The
	Dual        *Reference // If (M or N) is a blank node, this is a pointer to its reference struct
}

func (ref *Reference) String() string {
	return fmt.Sprintf(
		"%s/%d:%d {%s %s} :: %s",
		ref.Graph, ref.Index, ref.Permutation,
		ref.M.GetValue(), ref.N.GetValue(),
		ref.Cursor.String(),
	)
}

func (ref *Reference) close() {
	if ref.Cursor != nil {
		if ref.Cursor.Iterator != nil {
			ref.Cursor.Iterator.Close()
			ref.Cursor.Iterator = nil
		}
	}
}

func (ref *Reference) assembleCountKey(tree map[string]*Assignment, major bool) ([]byte, bool) {
	m := marshalReferenceNode(ref.M, tree)
	n := marshalReferenceNode(ref.N, tree)
	var key []byte
	if m != nil && n != nil {
		if major {
			permutation := (ref.Permutation + 1) % 3
			prefix := MajorPrefixes[permutation]
			key = assembleKey(prefix, m, n, nil)
		} else {
			permutation := (ref.Permutation + 2) % 3
			prefix := MinorPrefixes[permutation]
			key = assembleKey(prefix, n, m, nil)
		}
	} else if m != nil && n == nil {
		permutation := (ref.Permutation + 1) % 3
		prefix := IndexPrefixes[permutation]
		key = assembleKey(prefix, m, nil, nil)
	} else if m == nil && n != nil {
		permutation := (ref.Permutation + 2) % 3
		prefix := IndexPrefixes[permutation]
		key = assembleKey(prefix, n, nil, nil)
	}
	return key, !major
}

func marshalReferenceNode(node ld.Node, index map[string]*Assignment) []byte {
	if blank, isBlank := node.(*ld.BlankNode); isBlank {
		if assignment, has := index[blank.Attribute]; has {
			return assignment.Value
		}
		return nil
	}
	return marshalNode("", node)
}

// A ReferenceSet is any slice of References
type referenceSet []*Reference

// Sort interface for ReferenceSet
func (refs referenceSet) Len() int                { return len(refs) }
func (refs referenceSet) Swap(a, b int)           { refs[a], refs[b] = refs[b], refs[a] }
func (refs referenceSet) Less(a, b int) bool      { return refs[a].Cursor.Count < refs[b].Cursor.Count }
func (refs referenceSet) getCursor(i int) *Cursor { return refs[i].Cursor }

func (refs referenceSet) toCursorSet() CursorSet {
	cs := CursorSet{}
	for _, ref := range refs {
		cs = append(cs, ref.Cursor)
	}
	return cs
}

func (refs referenceSet) String() string {
	s := "[ "
	for i, ref := range refs {
		if i > 0 {
			s += ", "
		}
		s += ref.String()
	}
	return s + " ]"
}
