package styx

import (
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// NewIterator populates, scores, sorts, and connects a new constraint graph
func NewIterator(
	pattern []*ld.Quad,
	domain []*ld.BlankNode,
	index []ld.Node,
	tag TagScheme,
	txn *badger.Txn,
) (iter *Iterator, err error) {

	iter = &Iterator{
		graph:     make([][]Value, len(pattern)),
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

	// Copy the initial domain
	for i, node := range domain {
		iter.variables[i] = &variable{}
		iter.domain[i] = node
		iter.ids[node.Attribute] = i
	}

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
		variables[0], values[0], err = iter.parseNode(quad.Subject, tag)
		if err != nil {
			return
		}
		variables[1], values[1], err = iter.parseNode(quad.Predicate, tag)
		if err != nil {
			return
		}
		variables[2], values[2], err = iter.parseNode(quad.Object, tag)
		if err != nil {
			return
		}

		iter.graph[i] = values

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
			iter.constants = append(iter.constants, &constraint{
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

			err = iter.insertD1(variables[c.place], c, txn)
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
				err = iter.insertDZ(variables[q], c, txn)
				if err != nil {
					return
				}
			} else {
				neighbors := make([]*constraint, 3)
				a := &constraint{index: i, place: q, values: values, terms: terms, neighbors: neighbors}
				b := &constraint{index: i, place: r, values: values, terms: terms, neighbors: neighbors}
				neighbors[r], neighbors[q] = b, a
				err = iter.insertD2(variables[q], variables[r], a, txn)
				if err != nil {
					return
				}
				err = iter.insertD2(variables[r], variables[q], b, txn)
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
		for _, b := range iter.domain {
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
		n, _, err := nodeToValue(node, "", iter.values, tag, txn, nil, nil)
		if err != nil {
			return nil, err
		}
		indexValues[i] = n.Term()
	}

	// Score the variables
	for _, u := range iter.variables {
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
	if len(domain) < len(iter.domain)+1 {
		sort.Stable(iter)
		// Now we're in a tricky spot. g.domain and g.variables
		// have changed, but not g.ids or the variable constraint maps.
		transformation := make([]int, len(iter.domain))
		sortedIds := make(map[string]int, len(iter.domain))
		for i, p := range iter.domain {
			j := iter.ids[p.Attribute]
			transformation[j] = i
			sortedIds[p.Attribute] = i
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

	for i, u := range iter.variables {
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
	return iter, iter.Seek(indexValues)
}

// Seek advances the iterator to the first result
// greater than or equal to the given index path
func (g *Iterator) Seek(index []Term) (err error) {
	l := g.Len()

	var ok bool

	for i, u := range g.variables {
		if u.value == "" {
			root := u.root
			if i < len(index) {
				root = index[i]
			}
			for u.value = u.Seek(root); u.value == ""; u.value = u.Seek(root) {
				ok, err = g.tick(i, 0, g.cache)
				if err != nil || !ok {
					return
				}
			}
		}

		// We've got a non-nil value for u!
		g.push(u, i, l)
		for j, saved := range g.cache[:i] {
			if saved != nil {
				g.cache[j] = nil
				if i+1 < l {
					g.push(g.variables[j], i, l)
				}
			}
		}
	}

	return
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
