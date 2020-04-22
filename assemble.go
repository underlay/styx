package styx

import (
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
	rdf "github.com/underlay/go-rdfjs"
)

// NewIterator populates, scores, sorts, and connects a new constraint graph
func newIterator(
	query []*rdf.Quad,
	domain []rdf.Term,
	index []rdf.Term,
	tag TagScheme,
	txn *badger.Txn,
	dictionary Dictionary,
) (iter *Iterator, err error) {

	if domain == nil {
		domain = make([]rdf.Term, 0)
	}

	iter = &Iterator{
		query:      query,
		domain:     domain,
		pivot:      len(domain),
		constants:  make([]*constraint, 0),
		variables:  make([]*variable, len(domain)),
		ids:        make(map[string]int, len(domain)),
		unary:      newUnaryCache(),
		binary:     newBinaryCache(),
		tag:        tag,
		txn:        txn,
		dictionary: dictionary,
	}

	var split bool
	for i, node := range domain {
		if node.TermType() == rdf.VariableType {
			if split {
				return nil, ErrInvalidDomain
			}
		} else if node.TermType() == rdf.BlankNodeType {
			split = true
		} else {
			return nil, ErrInvalidDomain
		}

		value := node.String()
		iter.variables[i] = &variable{node: node}
		iter.ids[value] = i
	}

	// Check that the domian is valid
	if len(domain) < len(index) {
		err = ErrInvalidIndex
		return
	}

	for i, quad := range query {
		if quad.Graph().TermType() != rdf.DefaultGraphType {
			continue
		}

		variables := [3]*variable{}
		for p := 0; p < 3; p++ {
			variables[p] = iter.parseNode(quad[p])
		}

		degree := 0
		terms := [3]ID{}
		for p := 0; p < 3; p++ {
			if variables[p] == nil {
				terms[p], err = dictionary.GetID(quad[p], rdf.Default)
				if err != nil {
					return
				}
			} else {
				degree++
			}
		}

		if degree == 0 {
			iter.constants = append(iter.constants, &constraint{index: i, quad: quad})
		} else if degree == 1 {
			// Only one of the terms is a blank node, so this is a first-degree constraint.
			c := &constraint{
				index: i,
				quad:  quad,
				terms: terms,
			}

			for ; c.place < 3; c.place++ {
				if variables[c.place] != nil {
					break
				}
			}

			err = iter.insertD1(variables[c.place], c, txn)
			if err == ErrEndOfSolutions {
				iter.empty = true
				return iter, nil
			} else if err != nil {
				return
			}
		} else if degree == 2 {
			// Two of the terms is are blank nodes.
			// If they're the same blank node, then we insert one z-degree constraint.
			// If they're different, we insert two second-degree constraints.
			var p Permutation
			for ; p < 3; p++ {
				if variables[p] == nil {
					break
				}
			}

			q, r := (p+1)%3, (p+2)%3
			if variables[q] == variables[r] {
				c := &constraint{
					index: i,
					place: q,
					quad:  quad,
					terms: terms,
				}
				err = iter.insertDZ(variables[q], c, txn)
				if err == ErrEndOfSolutions {
					iter.empty = true
					return iter, nil
				} else if err != nil {
					return
				}
			} else {
				neighbors := make([]*constraint, 3)
				a := &constraint{index: i, place: q, quad: quad, terms: terms, neighbors: neighbors}
				b := &constraint{index: i, place: r, quad: quad, terms: terms, neighbors: neighbors}
				neighbors[r], neighbors[q] = b, a

				err = iter.insertD2(variables[q], variables[r], a, txn)
				if err == ErrEndOfSolutions {
					iter.empty = true
					return iter, nil
				} else if err != nil {
					return
				}

				err = iter.insertD2(variables[r], variables[q], b, txn)
				if err == ErrEndOfSolutions {
					iter.empty = true
					return iter, nil
				} else if err != nil {
					return
				}
			}
		} else if degree == 3 {
			return nil, fmt.Errorf("Cannot handle all-blank triple: %d", i)
		}
	}

	// Make sure that every node in the domain
	// actually occurs in the graph
	for _, u := range iter.variables {
		if len(u.cs) == 0 {
			err = ErrInvalidDomain
			return
		}
	}

	// Score the variables
	for _, u := range iter.variables {
		u.norm = 0

		for _, c := range u.cs {
			u.norm += uint64(c.count) * uint64(c.count)
		}

		u.score = float64(u.norm) / float64(u.cs.Len())

		u.Sort()

		u.root = u.cs.Seek(NIL)
		if u.root == NIL {
			err = ErrEmptyInterset
			return
		}

		// Set the initial value of each variable.
		// This will get overwritten to be NIL if/when
		// previous dependencies propagate their assignments.
		u.value = u.root
	}

	// Sorting keeps variables at indices less than iter.pivot in place
	if len(domain) < len(iter.domain)+1 {
		sort.Stable(iter)
		// Now we're in a tricky spot. iter.domain and iter.variables
		// have changed, but not iter.ids or the variable constraint maps.
		transformation := make([]int, len(iter.domain))
		sortedIds := make(map[string]int, len(iter.domain))
		for i, p := range iter.domain {
			value := p.String()
			j := iter.ids[value]
			transformation[j] = i
			sortedIds[value] = i
		}

		// set the new id map
		iter.ids = sortedIds

		// Now we relabel all the variables...
		for _, u := range iter.variables {
			d2 := make(constraintMap, len(u.edges))
			for i, cs := range u.edges {
				j := transformation[i]
				d2[j] = cs
			}
			u.edges = d2
		}
	}

	// Reset iter.pivot
	iter.pivot = len(iter.domain)
	for i, u := range iter.variables {
		// Set iter.pivot to be the index of the first blank node
		if i < iter.pivot && u.node.TermType() == rdf.BlankNodeType {
			iter.pivot = i
		}

		for j, cs := range u.edges {
			if j < i {
				// So these are connections that point "backward"
				// - i.e. q has already come before p.
				// These constraints are the ones that get pushed into,
				// and so they get deleted from the D2 map
				// (which is just for outgoing connections)
				cs.Close()
				for _, c := range cs {
					p := TernaryPrefixes[(c.place+1)%3]
					c.iterator = txn.NewIterator(badger.IteratorOptions{
						PrefetchValues: false,
						Prefix:         []byte{p},
					})
				}
				delete(u.edges, j)
			}
		}
	}

	// Assemble the dependency maps
	iter.in = make([][]int, len(iter.domain))
	iter.out = make([][]int, len(iter.domain))

	in := make([]map[int]bool, len(iter.domain))
	out := make([]map[int]bool, len(iter.domain))
	for i := range iter.domain {
		out[i] = map[int]bool{}
		for j := range iter.variables[i].edges {
			if in[j] == nil {
				in[j] = map[int]bool{i: true}
			} else {
				in[j][i] = true
			}
			for k := range in[i] {
				in[j][k] = true
			}
		}
	}

	// Invert the input map to get the output map
	for i, deps := range in {
		for j := range deps {
			out[j][i] = true
		}
	}

	// Sort the dependency maps
	for i := range iter.domain {
		iter.in[i] = make([]int, 0, len(in[i]))
		for j := range in[i] {
			iter.in[i] = append(iter.in[i], j)
		}
		sort.Ints(iter.in[i])

		iter.out[i] = make([]int, 0, len(out[i]))
		for j := range out[i] {
			iter.out[i] = append(iter.out[i], j)
		}

		sort.Ints(iter.out[i])
	}

	l := len(iter.domain)
	iter.cache = make([]*vcache, l)
	iter.blacklist = make([]bool, l)

	// Viola! We are returning a newly scored, sorted, and connected constraint graph.
	return iter, iter.Seek(index)
}

func (iter *Iterator) parseNode(node rdf.Term) *variable {
	if node.TermType() != rdf.VariableType && node.TermType() != rdf.BlankNodeType {
		return nil
	}

	value := node.String()
	if i, has := iter.ids[value]; has {
		u := iter.variables[i]
		return u
	}

	// if node.TermType() == rdf.VariableType {
	// 	iter.pivot++ // lol
	// }

	v := &variable{node: node}
	iter.ids[value] = len(iter.domain)
	iter.domain = append(iter.domain, node)
	iter.variables = append(iter.variables, v)
	return v
}
