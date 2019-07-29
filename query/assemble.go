package query

import (
	"encoding/binary"
	"errors"
	"sort"

	badger "github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

// MakeConstraintGraph populates, scores, sorts, and connects a new constraint graph
func MakeConstraintGraph(
	quads []*ld.Quad,
	graph string,
	indices []int,
	txn *badger.Txn,
) (g *ConstraintGraph, err error) {
	indexMap := types.IndexMap{}

	g = &ConstraintGraph{}

	for _, index := range indices {
		quad := quads[index]
		label := "@default"
		if quad.Graph != nil && quad.Graph.GetValue() != "" {
			label = quad.Graph.GetValue()
		}

		if label != graph {
			continue
		}

		s, S := getAttribute(quad.Subject)
		p, P := getAttribute(quad.Predicate)
		o, O := getAttribute(quad.Object)

		var c *Constraint
		if !S && !P && !O {
			continue
		} else if S && P && O {
			return nil, errors.New("Cannot handle all-blank triple")
		} else if (S && !P && !O) || (!S && P && !O) || (!S && !P && O) {
			// Only one of the terms is a blank node, so this is a first-degree constraint.
			c = &Constraint{Index: index}
			if S {
				c.Place = 0
				if c.M, c.m, err = getID(quad.Predicate, indexMap, txn); err != nil {
					return
				} else if c.N, c.n, err = getID(quad.Object, indexMap, txn); err != nil {
					return
				}
			} else if P {
				c.Place = 1
				if c.M, c.m, err = getID(quad.Object, indexMap, txn); err != nil {
					return
				} else if c.N, c.n, err = getID(quad.Subject, indexMap, txn); err != nil {
					return
				}
			} else if O {
				c.Place = 2
				if c.M, c.m, err = getID(quad.Subject, indexMap, txn); err != nil {
					return
				} else if c.N, c.n, err = getID(quad.Predicate, indexMap, txn); err != nil {
					return
				}
			}

			// (two of s, p, and o are the empty string)
			if err = g.insertD1(s+p+o, c, txn); err != nil {
				return
			}
		} else {
			// Two of the terms is are blank nodes.
			// If they're the same blank node, then we insert one z-degree constraint.
			// If they're different, we insert two second-degree constraints.
			if !O && s == p {
				c = &Constraint{Index: index, Place: pSP}
				if c.N, c.n, err = getID(quad.Object, indexMap, txn); err != nil {
					return
				}
				g.insertDZ(s, c, txn)
			} else if !P && o == s {
				c = &Constraint{Index: index, Place: pOS}
				if c.N, c.n, err = getID(quad.Predicate, indexMap, txn); err != nil {
					return
				}
				g.insertDZ(o, c, txn)
			} else if !S && p == o {
				c = &Constraint{Index: index, Place: pPO}
				if c.N, c.n, err = getID(quad.Subject, indexMap, txn); err != nil {
					return
				}
				g.insertDZ(p, c, txn)
			} else if S && P && !O {
				u, v := &Constraint{Index: index, Place: pS}, &Constraint{Index: index, Place: pP}
				if u.M, u.m, err = getID(quad.Predicate, indexMap, txn); err != nil {
					return
				} else if u.N, u.n, err = getID(quad.Object, indexMap, txn); err != nil {
					return
				} else if v.M, v.m, err = getID(quad.Object, indexMap, txn); err != nil {
					return
				} else if v.N, v.n, err = getID(quad.Subject, indexMap, txn); err != nil {
					return
				}

				u.Dual, v.Dual = v, u

				if err = g.insertD2(s, p, u, true, txn); err != nil {
					return
				} else if err = g.insertD2(p, s, v, false, txn); err != nil {
					return
				}
			} else if S && !P && O {
				u, v := &Constraint{Index: index, Place: pS}, &Constraint{Index: index, Place: pO}

				if u.M, u.m, err = getID(quad.Predicate, indexMap, txn); err != nil {
					return
				} else if u.N, u.n, err = getID(quad.Object, indexMap, txn); err != nil {
					return
				} else if v.M, v.m, err = getID(quad.Subject, indexMap, txn); err != nil {
					return
				} else if v.N, v.n, err = getID(quad.Predicate, indexMap, txn); err != nil {
					return
				}

				u.Dual, v.Dual = v, u

				if err = g.insertD2(s, o, u, true, txn); err != nil {
					return
				} else if err = g.insertD2(o, s, v, false, txn); err != nil {
					return
				}
			} else if !S && P && O {
				u, v := &Constraint{Index: index, Place: pP}, &Constraint{Index: index, Place: pO}

				if u.M, u.m, err = getID(quad.Object, indexMap, txn); err != nil {
					return
				} else if u.N, u.n, err = getID(quad.Subject, indexMap, txn); err != nil {
					return
				} else if v.M, v.m, err = getID(quad.Subject, indexMap, txn); err != nil {
					return
				} else if v.N, v.n, err = getID(quad.Predicate, indexMap, txn); err != nil {
					return
				}

				u.Dual, v.Dual = v, u

				if err = g.insertD2(p, o, u, true, txn); err != nil {
					return
				} else if err = g.insertD2(o, p, v, false, txn); err != nil {
					return
				}
			}
		}
	}

	// Score the variables
	for _, id := range g.Slice {
		if err = g.Index[id].Score(txn); err != nil {
			return
		}
	}

	// Sort self
	sort.Stable(g)

	// Invert the slice index
	g.Map = map[string]int{}
	for i, u := range g.Slice {
		g.Map[u] = i
		for v, cs := range g.Index[u].D2 {
			if _, has := g.Map[v]; has {
				cs.Close()
				for _, c := range cs {
					p := types.TriplePrefixes[(c.Place+1)%3]
					var prefix []byte
					if c.m != nil {
						prefix = types.AssembleKey(p, c.m, nil, nil)
					} else {
						prefix = types.AssembleKey(p, nil, nil, nil)
					}
					c.Iterator = txn.NewIterator(badger.IteratorOptions{
						PrefetchValues: false,
						Prefix:         prefix,
					})
				}
			}
		}
	}

	// Assemble the dependency maps
	g.In = map[string][]int{}
	g.Out = map[string][]int{}

	in := map[string]map[int]bool{}
	out := map[string]map[int]bool{}
	for i, u := range g.Slice {
		out[u] = map[int]bool{}
		if _, has := in[u]; !has {
			in[u] = map[int]bool{}
		}
		for v := range g.Index[u].D2 {
			if g.Map[v] > i {
				if _, has := in[v]; has {
					in[v][i] = true
				} else {
					in[v] = map[int]bool{i: true}
				}
				for j := range in[u] {
					in[v][j] = true
				}
			}
		}
	}

	// Invert the input map to get the output map
	for u, deps := range in {
		i := g.Map[u]
		for j := range deps {
			out[g.Slice[j]][i] = true
		}
	}

	// Sort the dependency maps
	for _, u := range g.Slice {
		g.In[u] = make([]int, 0, len(in[u]))
		for i := range in[u] {
			g.In[u] = append(g.In[u], i)
		}
		sort.Ints(g.In[u])

		g.Out[u] = make([]int, 0, len(out[u]))
		for i := range out[u] {
			g.Out[u] = append(g.Out[u], i)
		}
		sort.Ints(g.Out[u])
	}

	// Viola! We are returning a newly scored, sorted, and connected constraint graph.
	return
}

func getAttribute(node ld.Node) (string, bool) {
	if blank, isBlank := node.(*ld.BlankNode); isBlank {
		return blank.Attribute, true
	}
	return "", false
}

func getID(node ld.Node, indices types.IndexMap, txn *badger.Txn) (hasID HasID, bytes []byte, err error) {
	var index *types.Index
	if blank, isBlank := node.(*ld.BlankNode); isBlank {
		hasID = BlankNode(blank.Attribute)
		return
	} else if index, err = indices.Get(node, txn); err == badger.ErrKeyNotFound {
		return
	} else if err != nil {
		return
	}
	hasID, bytes = index, make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, index.GetId())
	return
}
