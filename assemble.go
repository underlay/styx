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
) (iterator *Iterator, err error) {

	g := &Iterator{
		constants: make([]*constraint, 0),
		variables: make([]*variable, len(domain)),
		domain:    make([]*ld.BlankNode, len(domain)),
		ids:       make(map[string]int, len(domain)),
		pivot:     len(domain),
		values:    newValueCache(),
		unary:     newUnaryCache(),
		binary:    newBinaryCache(),
		txn:       txn,
	}

	// if origin != "" {
	// 	g.origin, err = g.values.GetID(origin, txn)
	// 	if err != nil {
	// 		return
	// 	}
	// }

	// Copy the initial domain
	for i, node := range domain {
		g.variables[i] = &variable{}
		g.domain[i] = node
		g.ids[node.Attribute] = i
	}

	iterator = g

	// Check that the domian is valid
	if len(domain) < len(index) {
		err = ErrInvalidIndex
		return
	}

	for i, quad := range pattern {
		if quad.Graph != nil {
			value := quad.Graph.GetValue()
			if value != "" && value != "@default" {
				continue
			}
		}

		variables := [3]*variable{}
		values := make([]Value, 3)
		variables[0], values[0], err = g.parseNode(quad.Subject, tag)
		if err != nil {
			return
		}
		variables[1], values[1], err = g.parseNode(quad.Predicate, tag)
		if err != nil {
			return
		}
		variables[2], values[2], err = g.parseNode(quad.Object, tag)
		if err != nil {
			return
		}

		degree := 0
		terms := [3]Term{}
		for p := 0; p < 3; p++ {
			switch node := values[p].(type) {
			case *variable:
				degree++
			default:
				terms[p] = node.Term()
			}
		}

		if degree == 0 {
			g.constants = append(g.constants, &constraint{
				index:  i,
				place:  SPO,
				values: values,
			})
		} else if degree == 1 {
			// Only one of the terms is a blank node, so this is a first-degree constraint.
			c := &constraint{
				index:  i,
				values: values,
				terms:  terms,
			}

			for ; c.place < 3; c.place++ {
				if variables[c.place] != nil {
					break
				}
			}

			err = g.insertD1(variables[c.place], c, txn)
			if err != nil {
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
					index:  i,
					place:  q,
					values: values,
					terms:  terms,
				}
				err = g.insertDZ(variables[q], c, txn)
				if err != nil {
					return
				}
			} else {
				neighbors := make([]*constraint, 3)
				a := &constraint{index: i, place: q, values: values, terms: terms, neighbors: neighbors}
				b := &constraint{index: i, place: r, values: values, terms: terms, neighbors: neighbors}
				neighbors[r], neighbors[q] = b, a
				err = g.insertD2(variables[q], variables[r], a, txn)
				if err != nil {
					return
				}
				err = g.insertD2(variables[r], variables[q], b, txn)
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
		n, _, err := nodeToValue(node, "", g.values, tag, txn, nil, nil)
		if err != nil {
			return nil, err
		}
		indexValues[i] = n.Term()
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

	// Sorting g keeps variables at indices less than g.pivot in place
	if len(domain) < len(g.domain)+1 {
		sort.Stable(g)
		// Now we're in a tricky spot. g.domain and g.variables
		// have changed, but not g.ids or the variable constraint maps.
		transformation := make([]int, len(g.domain))
		sortedIds := make(map[string]int, len(g.domain))
		for i, p := range g.domain {
			j := g.ids[p.Attribute]
			transformation[j] = i
			sortedIds[p.Attribute] = i
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
	return iterator, g.initial(indexValues)
}

func (g *Iterator) parseNode(node ld.Node, tag TagScheme) (*variable, Value, error) {
	switch node := node.(type) {
	case *ld.BlankNode:
		if i, has := g.ids[node.Attribute]; has {
			u := g.variables[i]
			return u, u, nil
		}
		v := &variable{}
		g.ids[node.Attribute] = len(g.domain)
		g.domain = append(g.domain, node)
		g.variables = append(g.variables, v)
		return v, v, nil
	default:
		n, _, err := nodeToValue(node, "", g.values, tag, g.txn, nil, nil)
		return nil, n, err
	}
}
