package styx

import (
	"github.com/piprate/json-gold/ld"
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
*/

// An Assignment is a setting of a value to a variable
type Assignment struct {
	Constraints  []Reference    // constraints on layer siblings
	References   []Reference    // slice of references that force this assignment's value
	Value        []byte         // initialized to nil; filled in during actual search
	Iterator     []byte         // pointer for backtracking. also initialized nil
	Sources      [][]byte       // CID+graph+index (hopefully multiple for one value)
	Dependencies map[string]int // Indices of previous assignments. Could merge with refs?
	Count        uint64         // The sum of References.Count
}

// AssignmentMap is just a map of blank node ids to assignments
type AssignmentMap map[string]*Assignment

// An AssignmentStack is a slice of assignment maps
type AssignmentStack struct {
	deps map[string]int
	maps []AssignmentMap
}

// Reference is a reference in a dataset
type Reference struct {
	Graph       string
	Index       int
	Permutation uint8  // this is {0, 1, 2}, or 7 for no place at all ()
	M           string // These have to be strings in case they're blank node ids
	N           string // it breaks the convention of m and n being []byte slices, but oh well
	Count       uint64
}

// Codex is a map of refs
type Codex struct {
	Constant []Reference
	Single   map[string][]Reference
	Double   map[string]map[string][]Reference
	Triple   map[string]map[string]map[string][]Reference
}

func insertDouble(a string, b string, ref Reference, codex Codex) {
	if mapA, hasA := codex.Double[a]; hasA {
		if refsAB, hasAB := mapA[b]; hasAB {
			mapA[b] = append(refsAB, ref)
		} else {
			mapA[b] = []Reference{ref}
		}
	} else {
		codex.Double[a] = map[string][]Reference{b: []Reference{ref}}
	}
}

func insertTriple(a string, b string, c string, ref Reference, codex Codex) {
	if mapA, hasA := codex.Triple[a]; hasA {
		if mapAB, hasAB := mapA[b]; hasAB {
			if refsABC, hasABC := mapAB[c]; hasABC {
				mapAB[c] = append(refsABC, ref)
			} else {
				mapAB[c] = []Reference{ref}
			}
		} else {
			mapA[b] = map[string][]Reference{c: []Reference{ref}}
		}
	} else {
		codex.Triple[a] = map[string]map[string][]Reference{b: map[string][]Reference{c: []Reference{ref}}}
	}
}

func getCodex(dataset *ld.RDFDataset) Codex {
	codex := Codex{
		Constant: []Reference{},
		Single:   map[string][]Reference{},
		Double:   map[string]map[string][]Reference{},
		Triple:   map[string]map[string]map[string][]Reference{},
	}

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
				ref := Reference{graph, index, ConstantPermutation, "", "", 0}
				codex.Constant = append(codex.Constant, ref)
			} else if (A && !B && !C) || (!A && B && !C) || (!A && !B && C) {
				var permutation uint8
				if b != "" {
					permutation = 1
				} else if c != "" {
					permutation = 2
				}
				pivot := a + b + c
				ref := Reference{graph, index, permutation, "", "", 0}
				refs, has := codex.Single[pivot]
				if has {
					codex.Single[pivot] = append(refs, ref)
				} else {
					codex.Single[pivot] = []Reference{ref}
				}
			} else if A && B && !C {
				refA := Reference{graph, index, 0, b, "", 0}
				refB := Reference{graph, index, 1, "", a, 0}
				insertDouble(a, b, refA, codex)
				insertDouble(b, a, refB, codex)
			} else if A && !B && C {
				refA := Reference{graph, index, 0, "", c, 0}
				refC := Reference{graph, index, 2, a, "", 0}
				insertDouble(a, c, refA, codex)
				insertDouble(c, a, refC, codex)
			} else if !A && B && C {
				refB := Reference{graph, index, 1, c, "", 0}
				refC := Reference{graph, index, 2, "", b, 0}
				insertDouble(b, c, refB, codex)
				insertDouble(c, b, refC, codex)
			} else if A && B && C {
				refA := Reference{graph, index, 0, b, c, 0}
				refB := Reference{graph, index, 1, c, a, 0}
				refC := Reference{graph, index, 2, a, b, 0}
				insertTriple(a, b, c, refA, codex)
				insertTriple(a, c, b, refA, codex)
				insertTriple(b, a, c, refB, codex)
				insertTriple(b, c, a, refB, codex)
				insertTriple(c, a, b, refC, codex)
				insertTriple(c, b, a, refC, codex)
			}
		}
	}
	return codex
}

// Let's have dinner
func haveDinner(as AssignmentStack, codex Codex) (AssignmentStack, Codex) {
	am := AssignmentMap{}
	index := len(as.maps)
	as.maps = append(as.maps, am)

	// Every single gets added to the new assignment map
	for id, refs := range codex.Single {
		deps := map[string]int{}
		for _, ref := range refs {
			if ref.M != "" {
				deps[ref.M] = as.deps[ref.M]
			}
			if ref.N != "" {
				deps[ref.N] = as.deps[ref.N]
			}
		}
		am[id] = &Assignment{References: refs, Dependencies: deps}
		as.deps[id] = index
	}

	// There are no more singles left
	codex.Single = map[string][]Reference{}

	// for a := range as.deps { // <-- I think this was a typo but will leave for posterity
	for a := range am {

		// We're checking for entries of a:b:* NOT because we care about them (they get deleted),
		// but because we know that they mirror entries of b:a:*.
		if mapA, has := codex.Double[a]; has {
			delete(codex.Double, a) // `a` was promoted, we delete its entries
			for b := range mapA {
				if refs, has := codex.Double[b][a]; has {
					delete(codex.Double[b], a)
					if assignment, has := am[b]; has {
						if assignment.Constraints == nil {
							assignment.Constraints = refs
						} else {
							assignment.Constraints = append(assignment.Constraints, refs...)
						}
						if am[a].Constraints == nil {
							am[a].Constraints = mapA[b]
						} else {
							am[a].Constraints = append(am[a].Constraints, mapA[b]...)
						}
					} else if refsB, has := codex.Single[b]; has {
						codex.Single[b] = append(refsB, refs...)
					} else {
						codex.Single[b] = refs
					}
				}
			}
		}

		// Again, we're looking up a:b:c so that we can promote { b:a:c, b:c:a, c:a:b, c:b:a }
		// a:b:c and a:c:b get deleted.
		if mapA, has := codex.Triple[a]; has {
			delete(codex.Triple, a)
			for b, mapB := range mapA {
				_, hasB := am[b]
				delete(mapA, b)
				for c, refs := range mapB {
					if _, has := mapA[c]; has || b == c {
						/*
							# Diagonal iteration over double-nested map keys {u, v, w, x, y, z...}
							This and "delete(mapA, b)" are a hack to iterate "diagonally" over an unsorted map.
							Right now we have a double nested map iterator. We know that the key sets are the same
							(that for every (b, c) key pair down here there's also a (c, b) key pair in either
							the past or the future). BUT we don't want to double-insert the references that we find.

							So to consolidate the redundancy that we introduced when we called insertDouble() *twice*
							for each quad (and insertTriple() three times!), we delete the keys of the outer loop
							as we iterate, and break from the inner loop if we haven't deleted `c` from the outer loop yet.
							That way we get a unique iteration for every order-agnostic choice of pairs.
							As a special case, we want to also iterate when b == c.

							Here 'o' is "the inner loop code executes" and 'x' is "this break statement skips the iteration"
								u v w x y z ...
							u o x x x x x
							v o o x x x x
							w o o o x x x
							x o o o o x x
							y o o o o o x
							z o o o o o o
							...
						*/
						break
					}
					// Phew okay
					_, hasC := am[c]
					if hasB && hasC {
						// B and C are both assigned.
						if am[a].Constraints == nil {
							am[a].Constraints = refs
						} else {
							am[a].Constraints = append(am[a].Constraints, refs...)
						}
					} else if hasB {
						// Only B has been assigned; C is now a single.
						if refs, has := codex.Triple[c][a][b]; has {
							delete(codex.Triple[c][a], b)
							delete(codex.Triple[c][b], a)
							if refsC, has := codex.Single[c]; has {
								codex.Single[c] = append(refsC, refs...)
							} else {
								codex.Single[c] = refs
							}
						}
					} else if hasC {
						// Only C has been assigned; B is now a single
						if refs, has := codex.Triple[b][c][a]; has {
							delete(codex.Triple[b][c], a)
							delete(codex.Triple[b][a], c)
							if refsB, has := codex.Single[b]; has {
								codex.Single[b] = append(refsB, refs...)
							} else {
								codex.Single[b] = refs
							}
						}
					} else {
						// Neither have been assigned; both now doubles.
						// B
						if refsBCA, has := codex.Triple[b][c][a]; has {
							delete(codex.Triple[b][c], a)
							delete(codex.Triple[b][a], c)
							if refsBC, has := codex.Double[b][c]; has {
								codex.Double[b][c] = append(refsBC, refsBCA...)
							} else {
								codex.Double[b][c] = refsBCA
							}
						}
						// C
						if refsCAB, has := codex.Triple[c][a][b]; has {
							delete(codex.Triple[c][a], b)
							delete(codex.Triple[c][b], a)
							if refsCB, has := codex.Double[c][b]; has {
								codex.Double[c][b] = append(refsCB, refsCAB...)
							} else {
								codex.Double[c][b] = refsCAB
							}
						}
					}
				}
			}
		}
	}
	return as, codex
}

func getAssignmentStack(dataset *ld.RDFDataset) AssignmentStack {
	codex := getCodex(dataset)
	as := AssignmentStack{maps: []AssignmentMap{}, deps: map[string]int{}}
	for {
		as, codex = haveDinner(as, codex)
		if len(codex.Single) == 0 {
			break
		}
	}
	return as
}
