package styx

import (
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// MakeConstraintGraph populates, scores, sorts, and connects a new constraint graph
func MakeConstraintGraph(
	pattern []*ld.Quad,
	domain []*ld.BlankNode, index []ld.Node,
	tag TagScheme,
	txn *badger.Txn,
) (cursor Cursor, err error) {

	g := &cursorGraph{
		constants: make([]*constraint, 0),
		variables: make([]*variable, len(domain)),
		domain:    make([]*ld.BlankNode, len(domain)),
		ids:       make(map[variableNode]int, len(domain)),
		pivot:     len(domain),
		values:    newValueCache(),
		unary:     newUnaryCache(),
		binary:    newBinaryCache(),
		txn:       txn,
	}

	// Copy the initial domain
	for i, node := range domain {
		g.variables[i] = &variable{}
		g.domain[i] = node
		g.ids[variableNode(node.Attribute)] = i
	}

	cursor = g

	// Check that the domian is valid
	if len(domain) < len(index) {
		err = ErrInvalidIndex
		return
	}

	for i, quad := range pattern {
		blanks := [3]*ld.BlankNode{}
		nodes := &[3]term{}
		blanks[0], nodes[0], err = parseNode(quad.Subject, g.values, tag, g.txn)
		if err != nil {
			return
		}
		blanks[1], nodes[1], err = parseNode(quad.Predicate, g.values, tag, g.txn)
		if err != nil {
			return
		}
		blanks[2], nodes[2], err = parseNode(quad.Object, g.values, tag, g.txn)
		if err != nil {
			return
		}

		degree := 0
		values := [3]Term{}
		for p := 0; p < 3; p++ {
			switch node := nodes[p].(type) {
			case variableNode:
				degree++
			default:
				values[p] = node.term(nil)
			}
		}

		// s, S := quad.Subject.(*ld.BlankNode)
		// p, P := quad.Predicate.(*ld.BlankNode)
		// o, O := quad.Object.(*ld.BlankNode)

		if degree == 0 {
			g.constants = append(g.constants, &constraint{
				index: i,
				place: SPO,
				nodes: nodes,
			})
		} else if degree == 1 {
			// Only one of the terms is a blank node, so this is a first-degree constraint.
			c := &constraint{
				index:  i,
				nodes:  nodes,
				values: values,
			}

			for ; c.place < 3; c.place++ {
				if blanks[c.place] != nil {
					break
				}
			}

			err = g.insertD1(blanks[c.place], c, txn)
			if err != nil {
				return
			}
		} else if degree == 2 {
			// Two of the terms is are blank nodes.
			// If they're the same blank node, then we insert one z-degree constraint.
			// If they're different, we insert two second-degree constraints.
			var p Permutation
			for ; p < 3; p++ {
				if blanks[p] == nil {
					break
				}
			}

			q, r := (p+1)%3, (p+2)%3
			if blanks[q].Attribute == blanks[r].Attribute {
				c := &constraint{
					index:  i,
					place:  q,
					nodes:  nodes,
					values: values,
				}
				err = g.insertDZ(blanks[q], c, txn)
				if err != nil {
					return
				}
			} else {
				a := &constraint{index: i, place: q, nodes: nodes, values: values}
				b := &constraint{index: i, place: r, nodes: nodes, values: values}
				a.neighbors[r], b.neighbors[q] = b, a
				err = g.insertD2(blanks[q], blanks[r], a, txn)
				if err != nil {
					return
				}
				err = g.insertD2(blanks[r], blanks[q], b, txn)
				if err != nil {
					return
				}
			}
		} else if degree == 3 {
			return nil, fmt.Errorf("Cannot handle all-blank triple: %d", i)
		}
	}

	// Make sure that every node in the domain
	// actually occurs in the graph
	for _, a := range domain {
		err = ErrInvalidDomain
		for _, b := range g.domain {
			if a.Attribute == b.Attribute {
				err = nil
				break
			}
		}
		if err != nil {
			return
		}
	}

	// Set the .root values from indices first
	indexValues := make([]Term, len(index))
	for i, node := range index {
		var n term
		_, n, err = parseNode(node, g.values, tag, g.txn)
		if err != nil {
			return
		}
		indexValues[i] = n.term(nil)
	}

	// Score the variables
	for _, u := range g.variables {
		u.norm, u.size = 0, u.cs.Len()

		for _, c := range u.cs {
			u.norm += uint64(c.count) * uint64(c.count)
		}

		u.Sort()

		u.root = u.cs.Seek("")
		if u.root == "" {
			err = ErrEndOfSolutions
			return
		}

		// Set the initial value of each variable.
		// This will get overwritten to be nil if/when
		// previous dependencies propagate their assignments.
		u.value = u.root
	}

	// Reverse the domain (for REASONS)
	// for l, r := 0, len(domain)-1; l < r; l, r = l+1, r-1 {
	// 	domain[l], domain[r] = domain[r], domain[l]
	// }

	// Sorting g keeps variables at indices less than g.pivot in place
	if len(domain) < len(g.domain)+1 {
		sort.Stable(g)
		// Now we're in a tricky spot. g.domain and g.variables
		// have changed, but not g.ids or the variable constraint maps.
		transformation := make([]int, len(g.domain))
		sortedIds := make(map[variableNode]int, len(g.domain))
		for i, p := range g.domain {
			id := variableNode(p.Attribute)
			j := g.ids[id]
			transformation[j] = i
			sortedIds[id] = i
		}

		// set the new id map
		g.ids = sortedIds

		// Now we relabel all the variables...
		for _, u := range g.variables {
			d2 := make(constraintMap, len(u.edges))
			for i, cs := range u.edges {
				j := transformation[i]
				d2[j] = cs
			}
			u.edges = d2
		}
	}

	for i, u := range g.variables {
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
					// prefix := AssembleKey(p, true, c.mID)
					c.iterator = txn.NewIterator(badger.IteratorOptions{
						PrefetchValues: false,
						Prefix:         []byte{p},
						// Prefix:         prefix,
					})
				}
				delete(u.edges, j)
			}
		}
	}

	// Assemble the dependency maps
	g.in = make([][]int, len(g.domain))
	g.out = make([][]int, len(g.domain))

	in := make([]map[int]bool, len(g.domain))
	out := make([]map[int]bool, len(g.domain))
	for i := range g.domain {
		out[i] = map[int]bool{}
		for j := range g.variables[i].edges {
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
	for i := range g.domain {
		g.in[i] = make([]int, 0, len(in[i]))
		for j := range in[i] {
			g.in[i] = append(g.in[i], j)
		}
		sort.Ints(g.in[i])

		g.out[i] = make([]int, 0, len(out[i]))
		for j := range out[i] {
			g.out[i] = append(g.out[i], j)
		}

		sort.Ints(g.out[i])
	}

	l := len(g.domain)
	g.cache = make([]*V, l)
	g.blacklist = make([]bool, l)

	// Viola! We are returning a newly scored, sorted, and connected constraint graph.
	return cursor, g.initial(indexValues)
}

func parseNode(node ld.Node, values valueCache, tag TagScheme, txn *badger.Txn) (*ld.BlankNode, term, error) {
	switch node := node.(type) {
	case *ld.BlankNode:
		return node, variableNode(node.Attribute), nil
	default:
		n, _, err := nodeToValue(node, "", values, tag, txn, nil, nil)
		return nil, n, err
	}
}
