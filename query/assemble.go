package query

import (
	"encoding/binary"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

const pC uint8 = 255 // zoot zoot
const pS uint8 = 0
const pP uint8 = 1
const pO uint8 = 2
const pSP uint8 = 3 // it's important that pSP % 3 == pS, etc
const pPO uint8 = 4
const pOS uint8 = 5
const pSPO uint8 = 9

// MakeConstraintGraph populates, scores, sorts, and connects a new constraint graph
func MakeConstraintGraph(
	quads []*ld.Quad,
	graph []int,
	domain []string,
	cursor []ld.Node,
	uri types.URI,
	txn *badger.Txn,
) (g *ConstraintGraph, err error) {

	indexMap := types.IndexMap{}

	g = &ConstraintGraph{}

	for _, i := range graph {
		quad := quads[i]

		s, S := getAttribute(quad.Subject)
		p, P := getAttribute(quad.Predicate)
		o, O := getAttribute(quad.Object)

		if !S && !P && !O {
			continue
		} else if S && P && O {
			return nil, fmt.Errorf("Cannot handle all-blank triple: %d", i)
		} else if (S && !P && !O) || (!S && P && !O) || (!S && !P && O) {
			// Only one of the terms is a blank node, so this is a first-degree constraint.
			c := &Constraint{Index: i}
			if S {
				c.Place = 0
				if c.M, c.m, err = getID(quad.Predicate, indexMap, uri, txn); err != nil {
					return
				} else if c.N, c.n, err = getID(quad.Object, indexMap, uri, txn); err != nil {
					return
				}
			} else if P {
				c.Place = 1
				if c.M, c.m, err = getID(quad.Object, indexMap, uri, txn); err != nil {
					return
				} else if c.N, c.n, err = getID(quad.Subject, indexMap, uri, txn); err != nil {
					return
				}
			} else if O {
				c.Place = 2
				if c.M, c.m, err = getID(quad.Subject, indexMap, uri, txn); err != nil {
					return
				} else if c.N, c.n, err = getID(quad.Predicate, indexMap, uri, txn); err != nil {
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
				c := &Constraint{Index: i, Place: pSP}

				if c.N, c.n, err = getID(quad.Object, indexMap, uri, txn); err != nil {
					return
				}

				g.insertDZ(s, c, txn)
			} else if !P && o == s {
				c := &Constraint{Index: i, Place: pOS}

				if c.N, c.n, err = getID(quad.Predicate, indexMap, uri, txn); err != nil {
					return
				}

				g.insertDZ(o, c, txn)
			} else if !S && p == o {
				c := &Constraint{Index: i, Place: pPO}

				if c.N, c.n, err = getID(quad.Subject, indexMap, uri, txn); err != nil {
					return
				}

				g.insertDZ(p, c, txn)
			} else if S && P && !O {
				u, v := &Constraint{Index: i, Place: pS}, &Constraint{Index: i, Place: pP}

				if u.M, u.m, err = getID(quad.Predicate, indexMap, uri, txn); err != nil {
					return
				} else if u.N, u.n, err = getID(quad.Object, indexMap, uri, txn); err != nil {
					return
				} else if v.M, v.m, err = getID(quad.Object, indexMap, uri, txn); err != nil {
					return
				} else if v.N, v.n, err = getID(quad.Subject, indexMap, uri, txn); err != nil {
					return
				}

				u.Dual, v.Dual = v, u

				if err = g.insertD2(s, p, u, txn); err != nil {
					return
				} else if err = g.insertD2(p, s, v, txn); err != nil {
					return
				}
			} else if S && !P && O {
				u, v := &Constraint{Index: i, Place: pS}, &Constraint{Index: i, Place: pO}

				if u.M, u.m, err = getID(quad.Predicate, indexMap, uri, txn); err != nil {
					return
				} else if u.N, u.n, err = getID(quad.Object, indexMap, uri, txn); err != nil {
					return
				} else if v.M, v.m, err = getID(quad.Subject, indexMap, uri, txn); err != nil {
					return
				} else if v.N, v.n, err = getID(quad.Predicate, indexMap, uri, txn); err != nil {
					return
				}

				u.Dual, v.Dual = v, u

				if err = g.insertD2(s, o, u, txn); err != nil {
					return
				} else if err = g.insertD2(o, s, v, txn); err != nil {
					return
				}
			} else if !S && P && O {
				u, v := &Constraint{Index: i, Place: pP}, &Constraint{Index: i, Place: pO}

				if u.M, u.m, err = getID(quad.Object, indexMap, uri, txn); err != nil {
					return
				} else if u.N, u.n, err = getID(quad.Subject, indexMap, uri, txn); err != nil {
					return
				} else if v.M, v.m, err = getID(quad.Subject, indexMap, uri, txn); err != nil {
					return
				} else if v.N, v.n, err = getID(quad.Predicate, indexMap, uri, txn); err != nil {
					return
				}

				u.Dual, v.Dual = v, u

				if err = g.insertD2(p, o, u, txn); err != nil {
					return
				} else if err = g.insertD2(o, p, v, txn); err != nil {
					return
				}
			}
		}
	}

	// Check that the domian is valid
	if len(domain) < len(cursor) {
		err = ErrInvalidDomain
		return
	}

	for _, a := range domain {
		err = ErrInvalidDomain
		for _, b := range g.Domain {
			if a == b {
				err = nil
				break
			}
		}
		if err != nil {
			return
		}
	}

	delta := len(domain) - len(cursor)
	for i, node := range cursor {
		p := domain[i+delta]
		j := g.Map[p]
		v := g.Variables[j]
		if _, v.Root, err = getID(node, indexMap, uri, txn); err != nil {
			return
		}
	}

	// Score the variables
	for _, u := range g.Variables {
		if err = u.Score(txn); err != nil {
			return
		}

		// Set the initial value of each variable.
		// This will get overwritten to be nil if/when
		// previous dependencies propagate their assignments.
		u.Value = u.Root
	}

	// Reverse the domain (for REASONS)
	for l, r := 0, len(domain)-1; l < r; l, r = l+1, r-1 {
		domain[l], domain[r] = domain[r], domain[l]
	}

	if len(domain) == len(g.Domain) {
		g.Pivot = len(domain)

		nextMap := make(map[string]int, len(domain))
		for i, p := range domain {
			nextMap[p] = i
		}

		transformation := make([]int, len(domain))
		for i, p := range g.Domain {
			transformation[i] = nextMap[p]
		}

		variables := make([]*Variable, len(domain))
		for i, u := range g.Variables {
			j := transformation[i]
			variables[j] = u
			u.relabel(transformation)
		}

		g.Map = nextMap
		g.Domain = domain
		g.Variables = variables
	} else if len(domain) == 0 {
		// If the domain is length 0 (aka not provided),
		// we intepret it as being the entire implied g.Domain
		g.Pivot = len(g.Domain)
		sort.Stable(g)

		transformation := make([]int, len(g.Domain))
		nextMap := make(map[string]int, len(g.Domain))
		for i, p := range g.Domain {
			j := g.Map[p]
			transformation[j] = i
			nextMap[p] = i
		}

		for _, u := range g.Variables {
			u.relabel(transformation)
		}

		g.Map = nextMap
	} else {
		g.Pivot = len(domain)
		transformation := make([]int, len(g.Domain))
		nextMap := make(map[string]int, len(g.Domain))
		variables := make([]*Variable, len(domain))
		for i, p := range domain {
			j := g.Map[p]
			variables[i] = g.Variables[j]
			transformation[j] = i
			nextMap[p] = i
		}

		// Get the variables outside the domain
		l := len(g.Domain) - len(domain)
		complement := make([]string, 0, l)
		complementVars := make([]*Variable, 0, l)
		for i, p := range g.Domain {
			if _, has := nextMap[p]; !has {
				complement = append(complement, p)
				complementVars = append(complementVars, g.Variables[i])
			}
		}

		// Now sort _just those variables_, which is a little hacky.
		g.Domain = complement
		g.Variables = complementVars
		sort.Stable(g)

		for i, p := range complement {
			j := len(domain) + i
			transformation[len(domain)+i] = j
			nextMap[p] = j
		}

		// Okay, now concatenate the provided domain (most significant)
		// and the sorted complement (least significant)
		g.Domain = append(domain, complement...)
		g.Variables = append(variables, complementVars...)

		for _, u := range g.Variables {
			u.relabel(transformation)
		}
	}

	// Invert the slice index
	for i, u := range g.Variables {
		for j, cs := range u.D2 {
			if j < i {
				// So these are connections that point "backward"
				// - i.e. q has already come before p.
				// These constraints are the ones that get pushed into,
				// and so they get deleted from the D2 map
				// (which is just for outgoing connections)
				cs.Close()
				for _, c := range cs {
					p := types.TriplePrefixes[(c.Place+1)%3]
					prefix := types.AssembleKey(p, c.m, nil, nil)
					c.Iterator = txn.NewIterator(badger.IteratorOptions{
						PrefetchValues: false,
						Prefix:         prefix,
					})
				}
				delete(u.D2, j)
			}
		}
	}

	// Assemble the dependency maps
	g.In = make([][]int, len(g.Domain))
	g.Out = make([][]int, len(g.Domain))

	in := make([]map[int]bool, len(g.Domain))
	out := make([]map[int]bool, len(g.Domain))
	for i := range g.Domain {
		out[i] = map[int]bool{}
		for j := range g.Variables[i].D2 {
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
	for i := range g.Domain {
		g.In[i] = make([]int, 0, len(in[i]))
		for j := range in[i] {
			g.In[i] = append(g.In[i], j)
		}
		sort.Ints(g.In[i])

		g.Out[i] = make([]int, 0, len(out[i]))
		for j := range out[i] {
			g.Out[i] = append(g.Out[i], j)
		}
		sort.Ints(g.Out[i])
	}

	// Viola! We are returning a newly scored, sorted, and connected constraint graph.
	return
}

func getAttribute(node ld.Node) (attribute string, is bool) {
	var blank *ld.BlankNode
	if blank, is = node.(*ld.BlankNode); is {
		attribute = blank.Attribute
	}
	return
}

func getID(node ld.Node, indexMap types.IndexMap, uri types.URI, txn *badger.Txn) (hasID HasID, id I, err error) {
	var index *types.Index
	if blank, isBlank := node.(*ld.BlankNode); isBlank {
		hasID = VariableNode(blank.Attribute)
		return
	} else if index, err = indexMap.Get(node, uri, txn); err == badger.ErrKeyNotFound {
		return
	} else if err != nil {
		return
	}
	hasID, id = index, make([]byte, 8)
	binary.BigEndian.PutUint64(id, index.GetId())
	return
}
