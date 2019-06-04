package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"
)

// Codex is a map of references. A codex is always relative to a specific variable.
type Codex struct {
	Constraint ReferenceSet // an almost-always empty list of refs that include the codex's variable more than once
	Single     ReferenceSet // list of refs that include the variable once, and two known values
	Double     ReferenceMap // list of refs that include the variable once, one unknown value, and one known value
	Root       []byte       // the first possible value of the assignment, based on every static references
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

// Initialize cursors, counts, value root, and heuristics
func (codex *Codex) Initialize(major bool, txn *badger.Txn) (bool, error) {
	var err error
	codex.Count, codex.Norm, codex.Length = 0, 0, 0

	length := len(codex.Single) + len(codex.Double)
	cursors := make(CursorSet, 0, length)

	for _, ref := range codex.Single {
		major, err = ref.Initialize(major, txn)
		if err != nil {
			return major, err
		}
		codex.updateHeuristics(ref)
		cursors = append(cursors, ref.Cursor)
	}

	for _, refs := range codex.Double {
		for _, ref := range refs {
			major, err = ref.Initialize(major, txn)
			if err != nil {
				return major, err
			}
			codex.updateHeuristics(ref)
			cursors = append(cursors, ref.Cursor)
		}
	}

	sort.Stable(cursors)

	codex.Root = cursors.Seek(nil)
	if codex.Root == nil {
		return major, fmt.Errorf("Value root nil: %v", codex)
	}

	return major, nil
}

// A CodexMap associates ids with Codex maps.
type CodexMap struct {
	Value map[uint64]*Index
	Index map[string]*Codex
	Slice []string
}

// Close calls codex.close() on its children
func (codexMap *CodexMap) Close() {
	if codexMap == nil {
		return
	}
	for _, id := range codexMap.Slice {
		codexMap.Index[id].Close()
	}
}

// Sort interface functions
func (codexMap *CodexMap) Len() int { return len(codexMap.Slice) }
func (codexMap *CodexMap) Swap(a, b int) {
	codexMap.Slice[a], codexMap.Slice[b] = codexMap.Slice[b], codexMap.Slice[a]
}

// TODO: put more thought into the sorting heuristic.
// Right now the variables are sorted their norm: in
// increasing order of their lenght-normalized sum of
// the squares of the counts of all their references
// (past, present, and future).
func (codexMap *CodexMap) Less(a, b int) bool {
	A, B := codexMap.Index[codexMap.Slice[a]], codexMap.Index[codexMap.Slice[b]]
	return (float32(A.Norm) / float32(A.Length)) < (float32(B.Norm) / float32(B.Length))
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

// Initialize children and pass state between them
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
	valueMap := map[uint64]*Index{}
	constants := []*Reference{}
	codexMap := &CodexMap{Value: valueMap}
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
				ref, err := makeReference(graph, index, constantPermutation, nil, nil, indexMap, txn)
				if err != nil {
					return nil, nil, err
				}
				constants = append(constants, ref)
			} else if (A && !B && !C) || (!A && B && !C) || (!A && !B && C) {
				ref := &Reference{Graph: graph, Index: index}
				var mIndex, nIndex *Index
				ref.m = make([]byte, 8)
				ref.n = make([]byte, 8)
				if A {
					ref.Place = 0
					mIndex, err = indexMap.GetIndex(quad.Predicate, txn)
					if err != nil {
						return nil, nil, err
					}
					nIndex, err = indexMap.GetIndex(quad.Object, txn)
					if err != nil {
						return nil, nil, err
					}
				} else if B {
					ref.Place = 1
					mIndex, err = indexMap.GetIndex(quad.Object, txn)
					if err != nil {
						return nil, nil, err
					}
					nIndex, err = indexMap.GetIndex(quad.Subject, txn)
					if err != nil {
						return nil, nil, err
					}
				} else if C {
					ref.Place = 2
					mIndex, err = indexMap.GetIndex(quad.Subject, txn)
					if err != nil {
						return nil, nil, err
					}
					nIndex, err = indexMap.GetIndex(quad.Predicate, txn)
					if err != nil {
						return nil, nil, err
					}
				}

				ref.M = mIndex
				binary.BigEndian.PutUint64(ref.m, mIndex.GetId())

				ref.N = nIndex
				binary.BigEndian.PutUint64(ref.n, nIndex.GetId())

				pivot := a + b + c
				codex := codexMap.GetCodex(pivot)
				codex.Single = append(codex.Single, ref)
			} else if A && B && !C {
				if a == b {
					ref, err := makeReference(graph, index, permutationAB, nil, quad.Object, indexMap, txn)
					if err != nil {
						return nil, nil, err
					}
					codex := codexMap.GetCodex(a)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refA, err := makeReference(graph, index, permutationA, blankB, quad.Object, indexMap, txn)
					if err != nil {
						return nil, nil, err
					}
					refB, err := makeReference(graph, index, permutationB, quad.Object, blankA, indexMap, txn)
					if err != nil {
						return nil, nil, err
					}
					insertDouble(a, b, refA, codexMap)
					insertDouble(b, a, refB, codexMap)
					refA.Dual, refB.Dual = refB, refA
				}
			} else if A && !B && C {
				if c == a {
					ref, err := makeReference(graph, index, permutationCA, nil, quad.Predicate, indexMap, txn)
					if err != nil {
						return nil, nil, err
					}
					codex := codexMap.GetCodex(c)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refA, err := makeReference(graph, index, permutationA, quad.Predicate, blankC, indexMap, txn)
					if err != nil {
						fmt.Println("wow 5")
						return nil, nil, err
					}
					refC, err := makeReference(graph, index, permutationC, blankA, quad.Predicate, indexMap, txn)
					insertDouble(a, c, refA, codexMap)
					insertDouble(c, a, refC, codexMap)
					refA.Dual, refC.Dual = refC, refA
				}
			} else if !A && B && C {
				if b == c {
					ref, err := makeReference(graph, index, permutationBC, nil, quad.Subject, indexMap, txn)
					if err != nil {
						return nil, nil, err
					}
					codex := codexMap.GetCodex(b)
					codex.Constraint = append(codex.Constraint, ref)
				} else {
					refB, err := makeReference(graph, index, permutationB, blankC, quad.Subject, indexMap, txn)
					if err != nil {
						return nil, nil, err
					}
					refC, err := makeReference(graph, index, permutationC, quad.Subject, blankB, indexMap, txn)
					if err != nil {
						return nil, nil, err
					}
					insertDouble(b, c, refB, codexMap)
					insertDouble(c, b, refC, codexMap)
					refB.Dual, refC.Dual = refC, refB
				}
			} else if A && B && C {
				return nil, nil, errors.New("Cannot handle all-blank triple")
			}
		}
	}

	for _, index := range indexMap {
		id := index.GetId()
		codexMap.Value[id] = index
	}

	return constants, codexMap, nil
}

func makeReference(graph string, index int, place byte, m ld.Node, n ld.Node, indexMap IndexMap, txn *badger.Txn) (*Reference, error) {
	var M, N HasValue
	var mBytes, nBytes []byte

	if m != nil {
		if asBlank, isBlank := m.(*ld.BlankNode); isBlank {
			M = Variable(asBlank.Attribute)
		} else {
			mIndex, err := indexMap.GetIndex(m, txn)
			if err == badger.ErrKeyNotFound {
				return nil, err
			} else if err != nil {
				return nil, err
			}
			M = mIndex
			mBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(mBytes, mIndex.GetId())
		}
	}

	if n != nil {
		if asBlank, isBlank := n.(*ld.BlankNode); isBlank {
			N = Variable(asBlank.Attribute)
		} else {
			nIndex, err := indexMap.GetIndex(n, txn)
			if err == badger.ErrKeyNotFound {
				return nil, err
			} else if err != nil {
				return nil, err
			}
			N = nIndex
			nBytes = make([]byte, 8)
			binary.BigEndian.PutUint64(nBytes, nIndex.GetId())
		}
	}

	return &Reference{
		Graph:  graph,
		Index:  index,
		Place:  place,
		M:      M,
		m:      mBytes,
		N:      N,
		n:      nBytes,
		Cursor: &Cursor{},
		Dual:   nil,
	}, nil
}

func insertDouble(a string, b string, ref *Reference, codexMap *CodexMap) {
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
