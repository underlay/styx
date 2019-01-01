package styx

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"
)

/*
# Types of quads

Queries are compiled down to quads before resolution.
During resolution, the most important characteristic of each quad
is the number of blank nodes it references: 0, 1, 2, or possibly even 3.

## Case 0
Quads with no blank nodes have undefined interpretation; do nothing with them.
In the future it might make sense to create an interpretation like "constraining
allowed sources to those that also include this statement" but for now it's undefined.

## Case 1
Quads with exactly one blank node are our bread & butter.
No matter which index of the triple is blank, we can resolve it in a single db lookup.
We can even do this two different ways!

## Case 2
Quads with two blank nodes are tricky.
We should always be re-sorting the list of quads and solving for the case-1 quads first,
but if there are none left it's still possible to forge ahead off only case-2 constraints
with the hope that the pieces will fall into place.

We resolve case-2 constraints with Badger's Iterator function, assigning both values at once.
This is why it's important for db keys to be prefixed with their permutation id:
so we can start iterating relative to all three indices
(i.e. our single non-blank value could be the subject, predicate, or object).

## Case 3
Quads with three blank nodes are deemed impossible.
We *could* start iterating over every entry in the db,
but it ever gets to a point where there are only all-blank triples left,
we just reject the query as unresolvable.

One frustrating reality is that it's definitely possible to make an all-blank query
(whose only information content is in its adjacency matrix, not the labels)
that *does* have a unique solution - if fact, it's likely that *any* non-trivial
slice of the data from the universal graph is unique up to subgraph isomorphism.

But algorithms are hard and subgraph isomorphism is NP-hard.
https://en.wikipedia.org/wiki/Subgraph_isomorphism_problem
This isn't quite subgraph isomorphism since we want to allow two different variables
in the query to be resolved with the same value
("surjective subgraph homomorphism" or "subgraph epimorphism" if you really took notes)

So we're gonna have a slice of assigned Assignments
and a map of unassigned ones

two heuristics: one for backtracking and one for forward selection
(possibly really bad??? maybe actually okay)
{ o o o o o } * { o o o o o }
*/

// Reference is a reference in a dataset
type Reference struct {
	Graph       string
	Index       int
	Permutation uint8
	M           ld.Node
	N           ld.Node
	Count       uint64
}

func (ref *Reference) String() string {
	return fmt.Sprintf("%s/%d:%d {%s %s} * %d", ref.Graph, ref.Index, ref.Permutation, ref.M.GetValue(), ref.N.GetValue(), ref.Count)
}

type Constraint struct {
	Single []*Reference
	Double map[string][]*Reference
	Triple []*Reference
}

func NewConstraint() *Constraint {
	return &Constraint{
		Single: []*Reference{},
		Double: map[string][]*Reference{},
		Triple: []*Reference{},
	}
}

// Codex is a map of refs.
type Codex struct {
	Constraint *Constraint
	Single     []*Reference
	Double     map[string][]*Reference
	Triple     map[string]map[string][]*Reference
}

func (codex *Codex) String() string {
	val := fmt.Sprintln("--- codex ---")
	c, _ := json.MarshalIndent(codex.Constraint, "", "\t")
	s, _ := json.MarshalIndent(codex.Single, "", "\t")
	d, _ := json.MarshalIndent(codex.Double, "", "\t")
	t, _ := json.MarshalIndent(codex.Triple, "", "\t")
	val += fmt.Sprintf("%s\n%s\n%s\n%s\n", string(c), string(s), string(d), string(t))
	return val
}

func NewCodex() *Codex {
	return &Codex{
		Constraint: NewConstraint(),
		Single:     []*Reference{},
		Double:     map[string][]*Reference{},
		Triple:     map[string]map[string][]*Reference{},
	}
}

type CodexMap map[string]*Codex

func (codexMap CodexMap) GetCodex(id string) *Codex {
	codex, has := codexMap[id]
	if !has {
		codex = NewCodex()
		codexMap[id] = codex
	}
	return codex
}

// An Index into a Codex
type Index struct {
	start int
	end   int
	path  []string
	refs  []*Reference
}

// A Dependency is an index into a Codex
type Dependency struct {
	Constraint map[string][]*Index
	Single     map[string][]*Index            // these indices are either doubles or triples
	Double     map[string]map[string][]*Index // these indices are guaranteed to be Triples
}

func NewDependeny() *Dependency {
	return &Dependency{
		Constraint: map[string][]*Index{},
		Single:     map[string][]*Index{},
		Double:     map[string]map[string][]*Index{},
	}
}

// An Assignment is a setting of a value to a variable
type Assignment struct {
	Value      []byte
	Iterator   []byte
	Sources    []*Source
	Codex      *Codex // a codex is a general-purpose catalog of references???
	Dependency *Dependency
}

// func (assignment *Assignment) String() string {
// 	val := fmt.Sprintln("--- assignment ---")
// 	val += fmt.Sprintf("%v\n", assignment)
// 	return val
// }

// A Future is a sortable map of Assignments
type Future struct {
	constants []*Reference
	slice     []string
	index     map[string]*Assignment
}

func (future *Future) Len() int {
	return len(future.slice)
}

func (future *Future) Less(a int, b int) bool {
	// probably do something with this:
	// A := future.index[future.slice[a]]
	// B := future.index[future.slice[b]]
	return true
}

func (future *Future) Swap(a int, b int) {
	future.slice[a], future.slice[b] = future.slice[b], future.slice[a]
}

// Pop the last-sorted assignment out of the map
func (future *Future) Pop() (string, *Assignment) {
	index := future.Len() - 1
	id := future.slice[index]
	future.slice = future.slice[:index]
	assignment := future.index[id]
	delete(future.index, id)
	return id, assignment
}

// Push is not like normal pushing at all
func (future *Future) Push(next string) {
	for _, assignment := range future.index {
		// Constraints first. Only one option.
		constraint := assignment.Codex.Constraint
		if refs, has := constraint.Double[next]; has {
			delete(constraint.Double, next)
			start := len(constraint.Single)
			end := start + len(refs)
			constraint.Single = append(constraint.Single, refs...)
			index := &Index{start, end, nil, nil}
			if single, has := assignment.Dependency.Constraint[next]; has {
				assignment.Dependency.Constraint[next] = append(single, index)
			} else {
				assignment.Dependency.Constraint[next] = []*Index{index}
			}
		}

		// Now non-constraints
		if triple, has := assignment.Codex.Triple[next]; has {
			delete(assignment.Codex.Triple, next)
			for id, refs := range triple {
				mirrorRefs := assignment.Codex.Triple[id][next]
				delete(assignment.Codex.Triple[id], next)
				// Codex
				if double, has := assignment.Codex.Double[id]; has {
					assignment.Codex.Double[id] = append(double, mirrorRefs...)
				} else {
					assignment.Codex.Double[id] = mirrorRefs
				}
				// Dependency
				if single, has := assignment.Dependency.Single[id]; has {
					start := len(single)
					end := start + len(mirrorRefs)
					index := &Index{start, end, nil, refs}
					assignment.Dependency.Single[id] = append(single, index)
				} else {
					index := &Index{0, len(mirrorRefs), nil, refs}
					assignment.Dependency.Single[id] = []*Index{index}
				}
			}
			start := len(assignment.Codex.Single)
			end := start + len(refs)
			assignment.Codex.Single = append(assignment.Codex.Single, refs...)
			assignment.Dependency.Single[next] = [2]int{start, end}
		}
	}
}

// Count populates the Count field of all the references in the codex
func (future *Future) Count(past *Past, txn *badger.Txn) error {
	var err error
	count := make([]byte, 8)
	for _, assignment := range future.index {
		// Count constants
		// for _, ref := range assignment.Codex.Constant {
		// 	prefix := ValuePrefixes[ref.Permutation]
		// 	...
		// }
		// Count singles
		for _, ref := range assignment.Codex.Single {
			permutation := (ref.Permutation + 1) % 3
			prefix := MajorPrefixes[permutation] // we should really be alternating
			m := marshalReferenceNode(ref.M, past)
			if m == nil {
				return fmt.Errorf("Could not resolve M: %v", ref.M)
			}
			n := marshalReferenceNode(ref.N, past)
			if n == nil {
				return fmt.Errorf("Could not resolve N: %v", ref.N)
			}
			key := assembleKey(prefix, m, n, nil)
			count, err = populateReferenceCount(key, count, ref, txn)
			if err != nil {
				return err
			}
		}

		// Count doubles
		for _, refMap := range assignment.Codex.Double {
			for _, ref := range refMap {
				m := marshalReferenceNode(ref.M, past)
				n := marshalReferenceNode(ref.N, past)
				if m == nil && n == nil {
					return fmt.Errorf("Could not resolve either M or N: %v, %v", ref.M, ref.N)
				}
				var permutation uint8
				var a []byte
				if m != nil {
					permutation = (ref.Permutation + 1) % 3
					a = m
				} else if n != nil {
					permutation = (ref.Permutation + 2) % 3
					a = n
				}
				prefix := IndexPrefixes[permutation]
				key := assembleKey(prefix, a, nil, nil)
				count, err = populateReferenceCount(count, key, ref, txn)
				if err != nil {
					return err
				}
			}
		}
	}
	return err
}

// This is re-used between Major, Minor, and Index keys
func populateReferenceCount(count []byte, key []byte, ref *Reference, txn *badger.Txn) ([]byte, error) {
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		ref.Count = 0
	} else if err != nil {
		return nil, err
	} else if count, err = item.ValueCopy(count); err != nil {
		return nil, err
	} else {
		ref.Count = binary.BigEndian.Uint64(count)
	}
	return count, err
}

func marshalReferenceNode(node ld.Node, past *Past) []byte {
	if blank, isBlank := node.(*ld.BlankNode); isBlank {
		if assignment, has := past.index[blank.Attribute]; has {
			return assignment.Value
		}
		return nil
	}
	return []byte(node.GetValue())
}

// A Past is... also a sortable map of Assignments
type Past struct {
	slice []string
	index map[string]*Assignment
}

func (past *Past) Len() int {
	return len(past.slice)
}

func (past *Past) Less(a int, b int) bool {
	// probably do something with this:
	// A := past.index[past.slice[a]]
	// B := past.index[past.slice[b]]
	return true
}

func (past *Past) Swap(a int, b int) {
	past.slice[a], past.slice[b] = past.slice[b], past.slice[a]
}

// Pop here is different but hilarious
func (past *Past) Pop(future *Future) {

}

func insertDouble(a string, b string, ref *Reference, codexMap CodexMap) {
	codex := codexMap.GetCodex(a)
	if refs, has := codex.Double[b]; has {
		codex.Double[b] = append(refs, ref)
	} else {
		codex.Double[b] = []*Reference{ref}
	}
}

func insertTriple(a string, b string, c string, ref *Reference, codexMap CodexMap) {
	codex := codexMap.GetCodex(a)
	if mapB, hasB := codex.Triple[b]; hasB {
		if refs, has := mapB[c]; has {
			mapB[c] = append(refs, ref)
		} else {
			mapB[c] = []*Reference{ref}
		}
	} else {
		refs := []*Reference{ref}
		codex.Triple[b] = map[string][]*Reference{c: refs}
	}
}

func getInitialFuture(dataset *ld.RDFDataset) *Future {
	constants, codexMap := getInitalCodex(dataset)
	future := &Future{
		constants: constants,
		slice:     []string{},
		index:     map[string]*Assignment{},
	}
	for id, codex := range codexMap {
		future.slice = append(future.slice, id)
		future.index[id] = &Assignment{
			Value:      nil,
			Iterator:   nil,
			Sources:    []*Source{},
			Codex:      codex,
			Dependency: NewDependeny(),
		}
	}
	return future
}

func getInitalCodex(dataset *ld.RDFDataset) ([]*Reference, CodexMap) {
	constants := []*Reference{}
	codexMap := CodexMap{}

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
				ref := &Reference{graph, index, ConstantPermutation, nil, nil, 0}
				constants = append(constants, ref)
			} else if (A && !B && !C) || (!A && B && !C) || (!A && !B && C) {
				ref := &Reference{graph, index, 0, nil, nil, 0}
				if A {
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
				if blankA.Attribute == blankB.Attribute {
					ref := &Reference{graph, index, PermutationAB, nil, quad.Object, 0}
					codex := codexMap.GetCodex(blankA.Attribute)
					codex.Constraint.Single = append(codex.Constraint.Single, ref)
				} else {
					refA := &Reference{graph, index, PermutationA, blankB, quad.Object, 0}
					refB := &Reference{graph, index, PermutationB, quad.Object, blankA, 0}
					insertDouble(a, b, refA, codexMap)
					insertDouble(b, a, refB, codexMap)
				}
			} else if A && !B && C {
				if blankA.Attribute == blankC.Attribute {
					ref := &Reference{graph, index, PermutationCA, nil, quad.Predicate, 0}
					codex := codexMap.GetCodex(blankC.Attribute)
					codex.Constraint.Single = append(codex.Constraint.Single, ref)
				} else {
					refA := &Reference{graph, index, PermutationA, quad.Predicate, blankC, 0}
					refC := &Reference{graph, index, PermutationC, blankA, quad.Predicate, 0}
					insertDouble(a, c, refA, codexMap)
					insertDouble(c, a, refC, codexMap)
				}
			} else if !A && B && C {
				if blankB.Attribute == blankC.Attribute {
					ref := &Reference{graph, index, PermutationBC, nil, quad.Subject, 0}
					codex := codexMap.GetCodex(blankB.Attribute)
					codex.Constraint.Single = append(codex.Constraint.Single, ref)
				} else {
					refB := &Reference{graph, index, PermutationB, blankC, quad.Subject, 0}
					refC := &Reference{graph, index, PermutationC, quad.Subject, blankB, 0}
					insertDouble(b, c, refB, codexMap)
					insertDouble(c, b, refC, codexMap)
				}
			} else if A && B && C {
				var ab, bc, ca bool = blankA.Attribute == blankB.Attribute, blankB.Attribute == blankC.Attribute, blankC.Attribute == blankA.Attribute
				if ab && bc && ca {
					ref := &Reference{graph, index, PermutationABC, nil, nil, 0}
					codex := codexMap.GetCodex(blankA.Attribute)
					codex.Constraint.Triple = append(codex.Constraint.Triple, ref)
				} else if ab {
					ref := &Reference{graph, index, PermutationAB, nil, blankC, 0}
					codex := codexMap.GetCodex(blankA.Attribute)
					if double, has := codex.Constraint.Double[blankC.Attribute]; has {
						codex.Constraint.Double[blankC.Attribute] = append(double, ref)
					} else {
						codex.Constraint.Double[blankC.Attribute] = []*Reference{ref}
					}
				} else if bc {
					ref := &Reference{graph, index, PermutationBC, nil, blankA, 0}
					codex := codexMap.GetCodex(blankB.Attribute)
					if double, has := codex.Constraint.Double[blankA.Attribute]; has {
						codex.Constraint.Double[blankA.Attribute] = append(double, ref)
					} else {
						codex.Constraint.Double[blankA.Attribute] = []*Reference{ref}
					}
				} else if ca {
					ref := &Reference{graph, index, PermutationCA, nil, blankB, 0}
					codex := codexMap.GetCodex(blankC.Attribute)
					if double, has := codex.Constraint.Double[blankB.Attribute]; has {
						codex.Constraint.Double[blankB.Attribute] = append(double, ref)
					} else {
						codex.Constraint.Double[blankB.Attribute] = []*Reference{ref}
					}
				} else {
					refA := &Reference{graph, index, 0, blankB, blankC, 0}
					refB := &Reference{graph, index, 1, blankC, blankA, 0}
					refC := &Reference{graph, index, 2, blankA, blankB, 0}
					insertTriple(a, b, c, refA, codexMap)
					insertTriple(a, c, b, refA, codexMap)
					insertTriple(b, a, c, refB, codexMap)
					insertTriple(b, c, a, refB, codexMap)
					insertTriple(c, a, b, refC, codexMap)
					insertTriple(c, b, a, refC, codexMap)
				}
			}
		}
	}
	return constants, codexMap
}

func induct(past *Past, future *Future, txn *badger.Txn) error {
	err := future.Count(past, txn)
	if err != nil {
		return err
	}
	sort.Stable(future)
	id, assignment := future.Pop()
	solve(id, assignment)
	if assignment.Value == nil {
		past.Pop(future)
	} else {
		future.Push(id)
	}
	return nil
}

func solve(id string, assignment *Assignment) {

}
