package styx

import (
	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

/*
# Types of quads

Queries are compiled down to quads before resolution.
During resolution, the most important characteristic of each quad
is the number of blank nodes it references: 0, 1, 2, or possibly even 3.

## Case 0
Quads with no blank nodes have undefined interpretation; do nothing with them.

## Case 1
Quads with exactly one blank node are our bread & butter.
No matter which index of the triple is blank, we can resolve it in a single db lookup.

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
But algorithms are hard and graph isomorphism is NP-hard and maybe subgraph isomorphism
is too? I'm not sure. Either way, don't try this at home.
*/

type value struct {
	Value   string
	Sources []ld.Quad
}

type frame map[string]value

type index struct {
	graph string
	index int
	score int
}

type codex struct {
	one     int
	two     int
	three   int
	indices []index
}

func score(quad *ld.Quad) int {
	var result int
	if ld.IsBlankNode(quad.Subject) {
		result++
	}
	if ld.IsBlankNode(quad.Predicate) {
		result++
	}
	if ld.IsBlankNode(quad.Object) {
		result++
	}
	return result
}

func query(db *badger.DB, query interface{}) (interface{}, error) {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(nil)

	// Convert to RDF
	rdf, err := proc.ToRDF(query, options)
	if err != nil {
		return nil, err
	}

	dataset := rdf.(*ld.RDFDataset)
	var blankNodes map[string]*codex
	for g, quads := range dataset.Graphs {
		for i, quad := range quads {
			if ld.IsBlankNode(quad.Subject) {
				value := quad.Subject.GetValue()
				j := index{g, i, score(quad)}
				if _, has := blankNodes[value]; has {
					blankNodes[value].indices = append(blankNodes[value].indices, j)
				} else {
					one := 
					blankNodes[value] = &codex{0, 0, []index{j}}
				}
			}
		}
	}

	return nil, nil
}
