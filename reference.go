package styx

import (
	fmt "fmt"

	"github.com/piprate/json-gold/ld"
)

// Reference is a reference in a dataset
type Reference struct {
	Graph       string
	Index       int
	Permutation uint8
	M           ld.Node
	N           ld.Node
	Cursor      *Cursor
	Dual        *Reference
}

func (ref *Reference) String() string {
	return fmt.Sprintf("%s/%d:%d {%s %s}", ref.Graph, ref.Index, ref.Permutation, ref.M.GetValue(), ref.N.GetValue())
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
		prefix := MajorPrefixes[permutation]
		key = assembleKey(prefix, m, nil, nil)
	} else if m == nil && n != nil {
		permutation := (ref.Permutation + 2) % 3
		prefix := MajorPrefixes[permutation]
		key = assembleKey(prefix, n, nil, nil)
	}
	return key, !major
}

func marshalReferenceNode(node ld.Node, tree map[string]*Assignment) []byte {
	if blank, isBlank := node.(*ld.BlankNode); isBlank {
		if assignment, has := tree[blank.Attribute]; has {
			return assignment.Value
		}
		return nil
	}
	return []byte(node.GetValue())
}

// A ReferenceSet is any slice of References
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
