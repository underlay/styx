package query

import (
	"encoding/binary"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
	cid "github.com/ipfs/go-cid"
	ld "github.com/underlay/json-gold/ld"
	query "github.com/underlay/pkgs/query"

	types "github.com/underlay/styx/types"
)

// MakeConstraintGraph populates, scores, sorts, and connects a new constraint graph
func MakeConstraintGraph(
	pattern []*ld.Quad,
	domain []*ld.BlankNode, index []ld.Node,
	uri types.URI,
	txn *badger.Txn,
) (cursor query.Cursor, err error) {

	g := &cursorGraph{
		variables: make([]*variable, len(domain)),
		domain:    make([]*ld.BlankNode, len(domain)),
		ids:       make(map[string]int, len(domain)),
		pivot:     len(domain),
		indices:   types.NewIndexCache(),
		values:    types.NewValueCache(),
		uri:       uri,
		txn:       txn,
	}

	// Copy the initial domain
	for i, node := range domain {
		g.variables[i] = &variable{}
		g.domain[i] = node
		g.ids[node.Attribute] = i
	}

	cursor = g

	// Check that the domian is valid
	if len(domain) < len(index) {
		err = ErrInvalidDomain
		return
	}

	for i, quad := range pattern {
		s, S := quad.Subject.(*ld.BlankNode)
		p, P := quad.Predicate.(*ld.BlankNode)
		o, O := quad.Object.(*ld.BlankNode)

		if !S && !P && !O {
			continue
		} else if S && P && O {
			return nil, fmt.Errorf("Cannot handle all-blank triple: %d", i)
		} else if (S && !P && !O) || (!S && P && !O) || (!S && !P && O) {
			// Only one of the terms is a blank node, so this is a first-degree constraint.
			c := &constraint{index: i}

			// By default c.place == 0 == S
			u := s
			if P {
				u = p
				c.place = 1
			} else if O {
				u = o
				c.place = 2
			}

			m, n := types.GetNode(quad, (c.place+1)%3), types.GetNode(quad, (c.place+2)%3)

			c.m, c.mID, err = g.getID(m)
			if err != nil {
				return
			}

			c.n, c.nID, err = g.getID(n)
			if err != nil {
				return
			}

			err = g.insertD1(u, c, txn)
			if err != nil {
				return
			}
		} else {
			// Two of the terms is are blank nodes.
			// If they're the same blank node, then we insert one z-degree constraint.
			// If they're different, we insert two second-degree constraints.
			if !O && s == p {
				c := &constraint{index: i, place: types.SP}

				if c.n, c.nID, err = g.getID(quad.Object); err != nil {
					return
				}

				g.insertDZ(s, c, txn)
			} else if !P && o == s {
				c := &constraint{index: i, place: types.OS}

				if c.n, c.nID, err = g.getID(quad.Predicate); err != nil {
					return
				}

				g.insertDZ(o, c, txn)
			} else if !S && p == o {
				c := &constraint{index: i, place: types.PO}

				if c.n, c.nID, err = g.getID(quad.Subject); err != nil {
					return
				}

				g.insertDZ(p, c, txn)
			} else if S && P && !O {
				u, v := &constraint{index: i, place: types.S}, &constraint{index: i, place: types.P}

				if u.m, u.mID, err = g.getID(quad.Predicate); err != nil {
					return
				} else if u.n, u.nID, err = g.getID(quad.Object); err != nil {
					return
				} else if v.m, v.mID, err = g.getID(quad.Object); err != nil {
					return
				} else if v.n, v.nID, err = g.getID(quad.Subject); err != nil {
					return
				}

				u.dual, v.dual = v, u

				if err = g.insertD2(s, p, u, txn); err != nil {
					return
				} else if err = g.insertD2(p, s, v, txn); err != nil {
					return
				}
			} else if S && !P && O {
				u, v := &constraint{index: i, place: types.S}, &constraint{index: i, place: types.O}

				if u.m, u.mID, err = g.getID(quad.Predicate); err != nil {
					return
				} else if u.n, u.nID, err = g.getID(quad.Object); err != nil {
					return
				} else if v.m, v.mID, err = g.getID(quad.Subject); err != nil {
					return
				} else if v.n, v.nID, err = g.getID(quad.Predicate); err != nil {
					return
				}

				u.dual, v.dual = v, u

				if err = g.insertD2(s, o, u, txn); err != nil {
					return
				} else if err = g.insertD2(o, s, v, txn); err != nil {
					return
				}
			} else if !S && P && O {
				u, v := &constraint{index: i, place: types.P}, &constraint{index: i, place: types.O}

				if u.m, u.mID, err = g.getID(quad.Object); err != nil {
					return
				} else if u.n, u.nID, err = g.getID(quad.Subject); err != nil {
					return
				} else if v.m, v.mID, err = g.getID(quad.Subject); err != nil {
					return
				} else if v.n, v.nID, err = g.getID(quad.Predicate); err != nil {
					return
				}

				u.dual, v.dual = v, u

				if err = g.insertD2(p, o, u, txn); err != nil {
					return
				} else if err = g.insertD2(o, p, v, txn); err != nil {
					return
				}
			}
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
	for i, node := range index {
		// p := domain[i+delta]
		// j := g.ids[p.Attribute]
		// v := g.variables[j]
		v := g.variables[i]
		_, v.root, err = g.getID(node)
		if err != nil {
			return
		}
	}

	// Score the variables
	for _, u := range g.variables {
		err = u.Score(txn)
		if err != nil {
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
			d2 := make(constraintMap, len(u.d2))
			for i, cs := range u.d2 {
				j := transformation[i]
				d2[j] = cs
			}
			u.d2 = d2
		}
	}

	for i, u := range g.variables {
		for j, cs := range u.d2 {
			if j < i {
				// So these are connections that point "backward"
				// - i.e. q has already come before p.
				// These constraints are the ones that get pushed into,
				// and so they get deleted from the D2 map
				// (which is just for outgoing connections)
				cs.Close()
				for _, c := range cs {
					p := types.TriplePrefixes[(c.place+1)%3]
					prefix := types.AssembleKey(p, c.mID, nil, nil)
					c.iterator = txn.NewIterator(badger.IteratorOptions{
						PrefetchValues: false,
						Prefix:         prefix,
					})
				}
				delete(u.d2, j)
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
		for j := range g.variables[i].d2 {
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
	return cursor, g.initial()
}

func (g *cursorGraph) getID(n ld.Node) (node Node, id I, err error) {
	var index *types.Index
	if blank, isBlank := n.(*ld.BlankNode); isBlank {
		node = VariableNode(blank.Attribute)
		return
	}

	term := types.NodeToTerm(n, cid.Undef, g.uri)
	index, err = g.indices.Get(term, g.txn)
	if err == badger.ErrKeyNotFound { // hmm
		return
	} else if err != nil {
		return
	}

	node, id = index, make([]byte, 8)
	binary.BigEndian.PutUint64(id, index.GetId())
	return
}
