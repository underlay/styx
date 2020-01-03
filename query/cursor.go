package query

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
	types "github.com/underlay/styx/types"
)

type cursorGraph struct {
	variables []*variable
	domain    []*ld.BlankNode
	pivot     int
	ids       map[string]int
	cache     []*V
	blacklist []bool
	in        [][]int
	out       [][]int
	indices   types.IndexCache
	values    types.ValueCache
	uri       types.URI
	txn       *badger.Txn
}

var _ types.Cursor = (*cursorGraph)(nil)

func (g *cursorGraph) Graph() []*ld.Quad {
	return nil
}

func (g *cursorGraph) Get(node *ld.BlankNode) (n ld.Node) {
	if node == nil {
		return
	}

	i, has := g.ids[node.Attribute]
	if !has {
		return
	}

	v := g.variables[i]
	if v.value == nil || len(v.value) != 8 {
		return
	}

	id := binary.BigEndian.Uint64(v.value)
	value, err := g.values.Get(id, g.txn)
	if err != nil {
		return
	}

	return types.ValueToNode(value, g.values, g.uri, g.txn)
}

func (g *cursorGraph) Domain() []*ld.BlankNode {
	return g.domain
}

func (g *cursorGraph) Index() []ld.Node {
	index := make([]ld.Node, g.Len())
	for i, v := range g.variables {
		id := binary.BigEndian.Uint64(v.value)
		value, err := g.values.Get(id, g.txn)
		if err == nil {
			index[i] = types.ValueToNode(value, g.values, g.uri, g.txn)
		}
	}
	return index
}

func (g *cursorGraph) Next(node *ld.BlankNode) ([]*ld.BlankNode, error) {
	i := g.Len() - 1
	if node != nil {
		i = g.ids[node.Attribute]
	}
	tail, err := g.next(i)
	if err != nil {
		return nil, err
	}
	if tail == g.Len() {
		return nil, nil
	}
	return g.domain[tail:], nil
}

func (g *cursorGraph) Seek(index []ld.Node) error {
	return nil
}

func (g *cursorGraph) Close() {
	if g != nil {
		if g.variables != nil {
			for _, u := range g.variables {
				u.Close()
			}
		}
		if g.txn != nil {
			g.txn.Discard()
		}
	}
}

// // Close just calls Close on its child constraints
// func (g *CursorGraph) Close() {
// 	if g != nil && g.variables != nil {
// 		for _, u := range g.variables {
// 			u.Close()
// 		}
// 	}
// }

func (g *cursorGraph) String() string {
	s := "----- Constraint Graph -----\n"
	for i, id := range g.domain {
		s += fmt.Sprintf("---- %s ----\n%s\n", id, g.variables[i].String())
	}
	s += fmt.Sprintln("----- End of Constraint Graph -----")
	return s
}

// Sort interface functions
func (g *cursorGraph) Len() int { return len(g.domain) }
func (g *cursorGraph) Swap(a, b int) {
	// Swap is very important in assembling the graph.
	// The things it _does_ mutate are .variables and .domain.
	// It does NOT mutate .ids or the constraints, which is how we
	/// construct the transformation maps.
	if g.pivot < a && g.pivot < b {
		g.variables[a], g.variables[b] = g.variables[b], g.variables[a]
		g.domain[a], g.domain[b] = g.domain[b], g.domain[a]
	}
}

// TODO: put more thought into the sorting heuristic.
// Right now the variables are sorted their norm: in
// increasing order of their length-normalized sum of
// the squares of the counts of all their constraints (of any degree).
func (g *cursorGraph) Less(a, b int) bool {
	A, B := g.variables[a], g.variables[b]
	return (float32(A.norm) / float32(A.size)) < (float32(B.norm) / float32(B.size))
}

// variable retrieves a Variable or creates one if it doesn't exist.
func (g *cursorGraph) getVariable(node *ld.BlankNode) (v *variable, i int) {
	if index, has := g.ids[node.Attribute]; has {
		v, i = g.variables[index], index
	} else {
		v, i = &variable{}, len(g.domain)
		g.ids[node.Attribute] = i
		g.domain = append(g.domain, node)
		g.variables = append(g.variables, v)
	}
	return
}

func (g *cursorGraph) insertDZ(u *ld.BlankNode, c *constraint, txn *badger.Txn) (err error) {
	// For z-degree constraints we get the *count* with an index key
	// and set the *prefix* to a major key (although we could also use a minor key)

	variable, _ := g.getVariable(u)
	// if variable.DZ == nil {
	// 	variable.DZ = ConstraintSet{c}
	// } else {
	// 	variable.DZ = append(variable.DZ, c)
	// }
	if variable.cs == nil {
		variable.cs = constraintSet{c}
	} else {
		variable.cs = append(variable.cs, c)
	}

	place := (c.place + 2) % 3

	c.prefix = types.AssembleKey(types.MajorPrefixes[place], c.nID, nil, nil)

	if c.count = c.n.(*types.Index).Get(place); c.count == 0 {
		return ErrInitialCountZero
	}

	// Create a new badger.Iterator for the constraint
	c.iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.prefix,
	})

	return
}

func (g *cursorGraph) insertD1(u *ld.BlankNode, c *constraint, txn *badger.Txn) (err error) {
	// For first-degree constraints we get the *count* with a major key
	// and set the *prefix* to a triple key

	variable, _ := g.getVariable(u)
	// if variable.D1 == nil {
	// 	variable.D1 = ConstraintSet{c}
	// } else {
	// 	variable.D1 = append(variable.D1, c)
	// }
	if variable.cs == nil {
		variable.cs = constraintSet{c}
	} else {
		variable.cs = append(variable.cs, c)
	}

	// We rotate forward to get a major key, or backward to get a minor key.
	place := (c.place + 1) % 3
	c.prefix = types.AssembleKey(types.TriplePrefixes[place], c.mID, c.nID, nil)

	if c.count, err = c.getCount(txn); err != nil {
		return
	} else if c.count == 0 {
		return ErrInitialCountZero
	}

	// Create a new badger.Iterator for the constraint
	c.iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.prefix,
	})

	return
}

func (g *cursorGraph) insertD2(u, v *ld.BlankNode, c *constraint, txn *badger.Txn) (err error) {
	// For second-degree constraints we get the *count* with an index key
	// and set the *prefix* to either a major or minor key

	variable, _ := g.getVariable(u)
	if variable.d2 == nil {
		variable.d2 = constraintMap{}
	}

	_, j := g.getVariable(v)
	if cs, has := variable.d2[j]; has {
		variable.d2[j] = append(cs, c)
	} else {
		variable.d2[j] = constraintSet{c}
	}

	if variable.cs == nil {
		variable.cs = constraintSet{c}
	} else {
		variable.cs = append(variable.cs, c)
	}

	if index, is := c.m.(*types.Index); is {
		place := (c.place + 1) % 3
		c.count = index.Get(place)
		c.prefix = types.AssembleKey(types.MinorPrefixes[place], c.mID, nil, nil)
	} else if index, is := c.n.(*types.Index); is {
		place := (c.place + 2) % 3
		c.count = index.Get(place)
		c.prefix = types.AssembleKey(types.MajorPrefixes[place], c.nID, nil, nil)
	}

	if c.count == 0 {
		return ErrInitialCountZero
	}

	// Create a new badger.Iterator for the constraint
	c.iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.prefix,
	})

	return
}
