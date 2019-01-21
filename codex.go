package styx

import (
	"encoding/binary"
	"errors"
	fmt "fmt"
	"sort"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

// Codex is a map of refs. A codex is always relative to a specific variable.
type Codex struct {
	Constraint []*Reference            // an almost-always empty list of refs that include the codex's variable more than once
	Single     []*Reference            // list of refs that include the variable once, and two known values
	Double     map[string][]*Reference // list of refs that include the variable once, one unknown value, and one known value
	Count      uint64                  // The sum of key counts for Single and Double. Count is the only mutable value in a codex.
}

func (codex *Codex) String() string {
	var val string
	val += fmt.Sprintf("Constraint: %s\n", ReferenceSet(codex.Constraint).String())
	val += fmt.Sprintf("Singles: %s\n", ReferenceSet(codex.Single).String())
	val += fmt.Sprintln("Doubles:")
	for id, refs := range codex.Double {
		val += fmt.Sprintf("  %s: %s\n", id, ReferenceSet(refs).String())
	}
	val += fmt.Sprintf("Count: %d\n", codex.Count)
	return val
}

// A CodexMap associates ids with Codex maps.
type CodexMap struct {
	Index map[string]*Codex
	Slice []string
}

// Sort interface functions
func (c *CodexMap) Len() int      { return len(c.Slice) }
func (c *CodexMap) Swap(a, b int) { c.Slice[a], c.Slice[b] = c.Slice[b], c.Slice[a] }
func (c *CodexMap) Less(a, b int) bool {
	return c.Index[c.Slice[a]].Count < c.Index[c.Slice[b]].Count
}

// GetCodex retrieves an Codex or creates one if it doesn't exist.
func (c *CodexMap) GetCodex(id string) *Codex {
	if c.Index == nil {
		c.Index = map[string]*Codex{}
	}
	codex, has := c.Index[id]
	if !has {
		codex = &Codex{}
		c.Index[id] = codex
		c.Slice = append(c.Slice, id)
	}
	return codex
}

// InsertDouble into codex.Double
func (c *CodexMap) InsertDouble(a string, b string, ref *Reference) {
	codex := c.GetCodex(a)
	if codex.Double == nil {
		codex.Double = map[string][]*Reference{}
	}
	if refs, has := codex.Double[b]; has {
		codex.Double[b] = append(refs, ref)
	} else {
		codex.Double[b] = []*Reference{ref}
	}

}

// This is re-used between Major, Minor, and Index keys
func getCount(count []byte, key []byte, txn *badger.Txn) ([]byte, uint64, error) {
	item, err := txn.Get(key)
	fmt.Println("got count item", err)
	if err == badger.ErrKeyNotFound {
		return nil, 0, nil
	} else if err != nil {
		return nil, 0, err
	} else if count, err = item.ValueCopy(count); err != nil {
		return nil, 0, err
	} else {
		return count, binary.BigEndian.Uint64(count), nil
	}
}

// TODO: Make Sure you close the assignment.Present iterators some day
func (c *CodexMap) getAssignmentTree(txn *badger.Txn) ([]string, map[string]*Assignment, error) {
	var err error
	var major bool
	var key []byte
	count := make([]byte, 8)
	fmt.Println("getting the assignment tree", c.Slice)

	// Update the counts before sorting the codex map
	for _, codex := range c.Index {
		codex.Count = 0
		for _, ref := range codex.Single {
			key, major = ref.assembleCountKey(nil, major)
			fmt.Println("assembled single key", string(key))
			ref.Cursor = &Cursor{}
			count, ref.Cursor.Count, err = getCount(count, key, txn)
			if err != nil {
				fmt.Println("returning with error from getCount", err)
				return nil, nil, err
			} else if ref.Cursor.Count == 0 {
				return nil, nil, fmt.Errorf("Single reference count of zero: %s", ref.String())
			}
			codex.Count += ref.Cursor.Count
		}
		for _, refs := range codex.Double {
			for _, ref := range refs {
				key, major = ref.assembleCountKey(nil, major)
				fmt.Println("assembled double key", string(key))
				ref.Cursor = &Cursor{}
				count, ref.Cursor.Count, err = getCount(count, key, txn)
				if err != nil {
					return nil, nil, err
				} else if ref.Cursor.Count == 0 {
					return nil, nil, fmt.Errorf("Double reference count of zero: %s", ref.String())
				}
				codex.Count += ref.Cursor.Count
			}
		}
	}

	fmt.Println("sorted values:")
	printCodexMap(c)
	// Now sort the codex map
	sort.Stable(c)
	fmt.Println("the codex map has been sorted", c.Slice)

	index := map[string]*Assignment{}
	indexMap := map[string]int{}
	for i, id := range c.Slice {
		fmt.Println("Creating assignment for", id)
		indexMap[id] = i

		codex := c.Index[id]

		index[id] = &Assignment{
			Constraint: ReferenceSet(codex.Constraint),
			Present:    ReferenceSet(codex.Single),
			Past:       &Past{},
			Future:     map[string]ReferenceSet{},
		}

		deps := map[int]int{}
		var cursorCount int
		for dep, refs := range codex.Double {
			if j, has := indexMap[dep]; has {
				index[id].Past.Push(dep, j, refs)
				if j > deps[j] {
					deps[j] = j
				}
				for _, k := range index[dep].Dependencies {
					if j > deps[k] {
						deps[k] = j
					}
				}
			} else {
				index[id].Future[dep] = refs
				cursorCount += len(refs)
			}
		}

		index[id].Past.sortOrder()

		cursors := make(CursorSet, 0, cursorCount)
		for _, id := range index[id].Past.Slice {
			for k, ref := range index[id].Past.Index[id] {
				ref.Cursor.ID = id
				ref.Cursor.Index = k
				ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
				index[id].Past.Indices[id][k] = len(cursors)
				cursors = append(cursors, ref.Cursor)
			}
		}

		index[id].Past.Cursors = cursors

		index[id].Dependencies = make([]int, 0, len(deps))
		for j := range deps {
			index[id].Dependencies = append(index[id].Dependencies, j)
		}
		sort.Sort(index[id].Dependencies)

		index[id].setValueRoot(txn)
		if index[id].ValueRoot == nil {
			return nil, nil, fmt.Errorf("Assignment's static intersect is empty: %v", index[id])
		}
	}
	// return slice, index, nil
	return c.Slice, index, nil
}

func makeReference(graph string, index int, permutation byte, m ld.Node, n ld.Node) *Reference {
	return &Reference{graph, index, permutation, m, n, &Cursor{}, nil}
}

func getInitalCodexMap(dataset *ld.RDFDataset) ([]*Reference, *CodexMap, error) {
	constants := []*Reference{}
	codexMap := &CodexMap{}
	for graph, quads := range dataset.Graphs {
		for index, quad := range quads {
			var a, b, c string
			blankA, A := quad.Subject.(*ld.BlankNode)
			if A {
				a = blankA.Attribute
			}
			blankB, B := quad.Predicate.(*ld.BlankNode)
			if B {
				b = blankB.Attribute
			}
			blankC, C := quad.Object.(*ld.BlankNode)
			if C {
				c = blankC.Attribute
			}
			if !A && !B && !C {
				ref := makeReference(graph, index, ConstantPermutation, nil, nil)
				constants = append(constants, ref)
			} else if (A && !B && !C) || (!A && B && !C) || (!A && !B && C) {
				ref := &Reference{Graph: graph, Index: index}
				if A {
					ref.Permutation = 0
					ref.M = quad.Predicate
					ref.N = quad.Object
				} else if B {
					ref.Permutation = 1
					ref.M = quad.Object
					ref.N = quad.Subject
				} else if C {
					ref.Permutation = 2
					ref.M = quad.Subject
					ref.N = quad.Predicate
				}
				pivot := a + b + c
				codex := codexMap.GetCodex(pivot)
				codex.Single = append(codex.Single, ref)
			} else if A && B && !C {
				if a == b {
					ref := makeReference(graph, index, PermutationAB, nil, quad.Object)
					codex := codexMap.GetCodex(a)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refA := makeReference(graph, index, PermutationA, blankB, quad.Object)
					refB := makeReference(graph, index, PermutationB, quad.Object, blankA)
					codexMap.InsertDouble(a, b, refA)
					codexMap.InsertDouble(b, a, refB)
					refA.Dual, refB.Dual = refB, refA
				}
			} else if A && !B && C {
				if c == a {
					ref := makeReference(graph, index, PermutationCA, nil, quad.Predicate)
					codex := codexMap.GetCodex(c)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refA := makeReference(graph, index, PermutationA, quad.Predicate, blankC)
					refC := makeReference(graph, index, PermutationC, blankA, quad.Predicate)
					codexMap.InsertDouble(a, c, refA)
					codexMap.InsertDouble(c, a, refC)
					refA.Dual, refC.Dual = refC, refA
				}
			} else if !A && B && C {
				if b == c {
					ref := makeReference(graph, index, PermutationBC, nil, quad.Subject)
					codex := codexMap.GetCodex(b)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refB := makeReference(graph, index, PermutationB, blankC, quad.Subject)
					refC := makeReference(graph, index, PermutationC, quad.Subject, blankB)
					codexMap.InsertDouble(b, c, refB)
					codexMap.InsertDouble(c, b, refC)
					refB.Dual, refC.Dual = refC, refB
				}
			} else if A && B && C {
				return nil, nil, errors.New("Cannot handle all-blank triple")
			}
		}
	}
	return constants, codexMap, nil
}
