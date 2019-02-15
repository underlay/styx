package main

import (
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"
)

// Codex is a map of references. A codex is always relative to a specific variable.
type Codex struct {
	Constraint ReferenceSet // an almost-always empty list of refs that include the codex's variable more than once
	Single     ReferenceSet // list of refs that include the variable once, and two known values
	Double     ReferenceMap // list of refs that include the variable once, one unknown value, and one known value
	Norm       uint64       // The sum of squares of key counts of references.
	Length     int          // The total number of references
	Count      uint64
}

func (codex *Codex) String() string {
	var val string
	val += fmt.Sprintf("Constraint: %s\n", codex.Constraint.String())
	val += fmt.Sprintf("Singles: %s\n", codex.Single.String())
	val += fmt.Sprintln("Doubles:")
	for id, refs := range codex.Double {
		val += fmt.Sprintf("  %s: %s\n", id, refs.String())
	}
	val += fmt.Sprintf("Norm: %d\n", codex.Norm)
	val += fmt.Sprintf("Length: %d\n", codex.Length)
	return val
}

// Close calls reference.Close on its children
func (codex *Codex) Close() {
	for _, ref := range codex.Single {
		ref.Close()
	}
	for _, refs := range codex.Double {
		for _, ref := range refs {
			ref.Close()
		}
	}
}

// Update the codex heuristics with the updated reference
func (codex *Codex) updateHeuristics(ref *Reference) {
	codex.Count += ref.Cursor.Count
	codex.Norm += ref.Cursor.Count * ref.Cursor.Count
	codex.Length++
}

// Initialize cursors, counts, and heuristics
func (codex *Codex) Initialize(major bool, txn *badger.Txn) (bool, error) {
	var err error
	codex.Count, codex.Norm, codex.Length = 0, 0, 0
	for _, ref := range codex.Single {
		major, err = ref.Initialize(major, txn)
		if err != nil {
			return major, err
		}
		codex.updateHeuristics(ref)
	}

	for _, refs := range codex.Double {
		for _, ref := range refs {
			major, err = ref.Initialize(major, txn)
			if err != nil {
				return major, err
			}
			codex.updateHeuristics(ref)
		}
	}
	return major, nil
}

// A CodexMap associates ids with Codex maps.
type CodexMap struct {
	IndexMap map[string]*Index
	Index    map[string]*Codex
	Slice    []string
}

// Close calls codex.close() on its children
func (codexMap *CodexMap) Close() {
	for _, id := range codexMap.Slice {
		codexMap.Index[id].Close()
	}
}

// Sort interface functions
func (codexMap *CodexMap) Len() int { return len(codexMap.Slice) }
func (codexMap *CodexMap) Swap(a, b int) {
	codexMap.Slice[a], codexMap.Slice[b] = codexMap.Slice[b], codexMap.Slice[a]
}
func (codexMap *CodexMap) Less(a, b int) bool {
	A, B := codexMap.Index[codexMap.Slice[a]], codexMap.Index[codexMap.Slice[b]]
	return (float32(A.Norm) / float32(A.Length)) < (float32(B.Norm) / float32(B.Length))
	// return A.Count > B.Count
}

// GetCodex retrieves an Codex or creates one if it doesn't exist.
func (codexMap *CodexMap) GetCodex(id string) *Codex {
	if codexMap.Index == nil {
		codexMap.Index = map[string]*Codex{}
	}
	codex, has := codexMap.Index[id]
	if !has {
		codex = &Codex{}
		codexMap.Index[id] = codex
		codexMap.Slice = append(codexMap.Slice, id)
	}
	return codex
}

// insertDouble into codex.Double
func (codexMap *CodexMap) insertDouble(a string, b string, ref *Reference) {
	codex := codexMap.GetCodex(a)
	if codex.Double == nil {
		codex.Double = ReferenceMap{}
	}
	if refs, has := codex.Double[b]; has {
		codex.Double[b] = append(refs, ref)
	} else {
		codex.Double[b] = ReferenceSet{ref}
	}
}

// Initialize initializes its children and passes state between them
func (codexMap *CodexMap) Initialize(txn *badger.Txn) error {
	var err error
	var major bool
	for _, id := range codexMap.Slice {
		codex := codexMap.Index[id]
		major, err = codex.Initialize(major, txn)
		if err != nil {
			return err
		}
	}
	return nil
}

func getInitalCodexMap(dataset *ld.RDFDataset, txn *badger.Txn) ([]*Reference, *CodexMap, error) {
	var err error
	indexMap := IndexMap{}
	constants := []*Reference{}
	codexMap := &CodexMap{IndexMap: indexMap}
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
				ref := makeReference(graph, index, constantPermutation, nil, nil)
				constants = append(constants, ref)
			} else if (A && !B && !C) || (!A && B && !C) || (!A && !B && C) {
				ref := &Reference{Graph: graph, Index: index}
				if A {
					ref.Place = 0
					ref.M, err = indexMap.GetIndex(quad.Predicate, txn)
					if err != nil {
						return nil, nil, err
					}
					ref.N, err = indexMap.GetIndex(quad.Object, txn)
					if err != nil {
						return nil, nil, err
					}
				} else if B {
					ref.Place = 1
					ref.M, err = indexMap.GetIndex(quad.Object, txn)
					if err != nil {
						return nil, nil, err
					}
					ref.N, err = indexMap.GetIndex(quad.Subject, txn)
					if err != nil {
						return nil, nil, err
					}
				} else if C {
					ref.Place = 2
					ref.M, err = indexMap.GetIndex(quad.Subject, txn)
					if err != nil {
						return nil, nil, err
					}
					ref.N, err = indexMap.GetIndex(quad.Predicate, txn)
					if err != nil {
						return nil, nil, err
					}
				}
				pivot := a + b + c
				codex := codexMap.GetCodex(pivot)
				codex.Single = append(codex.Single, ref)
			} else if A && B && !C {
				if a == b {
					ref := makeReference(graph, index, permutationAB, nil, quad.Object)
					codex := codexMap.GetCodex(a)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refA := makeReference(graph, index, permutationA, blankB, quad.Object)
					refB := makeReference(graph, index, permutationB, quad.Object, blankA)
					codexMap.insertDouble(a, b, refA)
					codexMap.insertDouble(b, a, refB)
					refA.Dual, refB.Dual = refB, refA
				}
			} else if A && !B && C {
				if c == a {
					ref := makeReference(graph, index, permutationCA, nil, quad.Predicate)
					codex := codexMap.GetCodex(c)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refA := makeReference(graph, index, permutationA, quad.Predicate, blankC)
					refC := makeReference(graph, index, permutationC, blankA, quad.Predicate)
					codexMap.insertDouble(a, c, refA)
					codexMap.insertDouble(c, a, refC)
					refA.Dual, refC.Dual = refC, refA
				}
			} else if !A && B && C {
				if b == c {
					ref := makeReference(graph, index, permutationBC, nil, quad.Subject)
					codex := codexMap.GetCodex(b)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refB := makeReference(graph, index, permutationB, blankC, quad.Subject)
					refC := makeReference(graph, index, permutationC, quad.Subject, blankB)
					codexMap.insertDouble(b, c, refB)
					codexMap.insertDouble(c, b, refC)
					refB.Dual, refC.Dual = refC, refB
				}
			} else if A && B && C {
				return nil, nil, errors.New("Cannot handle all-blank triple")
			}
		}
	}
	return constants, codexMap, nil
}

func makeReference(graph string, index int, place byte, m ld.Node, n ld.Node) *Reference {
	var M, N HasValue
	if asBlank, isBlank := m.(*ld.BlankNode); isBlank {
		M = Variable(asBlank.Attribute)
	}
	if asBlank, isBlank := n.(*ld.BlankNode); isBlank {
		N = Variable(asBlank.Attribute)
	}
	return &Reference{
		Graph:  graph,
		Index:  index,
		Place:  place,
		M:      M,
		N:      N,
		Cursor: &Cursor{},
		Dual:   nil,
	}
}
