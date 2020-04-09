package styx

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// A Cursor is an interactive query interface
type Cursor interface {
	Len() int
	Graph() []*ld.Quad
	Get(node *ld.BlankNode) ld.Node
	Domain() []*ld.BlankNode
	Index() []ld.Node
	Next(node *ld.BlankNode) ([]*ld.BlankNode, error)
	Seek(index []ld.Node) error
	Close()
}

type cursorGraph struct {
	constants []*constraint
	variables []*variable
	domain    []*ld.BlankNode
	pivot     int
	ids       map[variableNode]int
	cache     []*V
	blacklist []bool
	in        [][]int
	out       [][]int
	values    valueCache
	binary    binaryCache
	unary     unaryCache
	txn       *badger.Txn
}

var _ Cursor = (*cursorGraph)(nil)

func (g *cursorGraph) Graph() []*ld.Quad {
	return nil
}

func (g *cursorGraph) Get(node *ld.BlankNode) (n ld.Node) {
	if node == nil {
		return
	}

	i, has := g.ids[variableNode(node.Attribute)]
	if !has {
		return
	}

	v := g.variables[i]
	if v.value == "" {
		return
	}

	value, _ := readValue([]byte(v.value))
	return value.Node("", g.values, g.txn)
}

func (g *cursorGraph) Domain() []*ld.BlankNode {
	return g.domain
}

func (g *cursorGraph) Index() []ld.Node {
	index := make([]ld.Node, g.Len())
	for i, v := range g.variables {
		value, _ := readValue([]byte(v.value))
		index[i] = value.Node("", g.values, g.txn)
	}
	return index
}

func (g *cursorGraph) Next(node *ld.BlankNode) ([]*ld.BlankNode, error) {
	i := g.Len() - 1
	if node != nil {
		i = g.ids[variableNode(node.Attribute)]
	}
	tail, err := g.next(i)
	if err == badger.ErrKeyNotFound {
		err = ErrEndOfSolutions
	}

	if err != nil {
		return nil, err
	}

	if tail == g.Len() {
		return nil, ErrEndOfSolutions
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

// getVariable retrieves a variable or creates one if it doesn't exist.
func (g *cursorGraph) getVariable(node *ld.BlankNode) (v *variable, i int) {
	if index, has := g.ids[variableNode(node.Attribute)]; has {
		v, i = g.variables[index], index
	} else {
		v, i = &variable{}, len(g.domain)
		g.ids[variableNode(node.Attribute)] = i
		g.domain = append(g.domain, node)
		g.variables = append(g.variables, v)
	}
	return
}

func (g *cursorGraph) insertDZ(u *ld.BlankNode, c *constraint, txn *badger.Txn) (err error) {
	variable, _ := g.getVariable(u)
	if variable.cs == nil {
		variable.cs = constraintSet{c}
	} else {
		variable.cs = append(variable.cs, c)
	}

	c.count, err = c.getCount(g.unary, g.binary, txn)
	if err != nil {
		return
	} else if c.count == 0 {
		return ErrEndOfSolutions
	}

	p := (c.place + 2) % 3
	c.prefix = assembleKey(BinaryPrefixes[p], true, c.values[p])

	// Create a new badger.Iterator for the constraint
	c.iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.prefix,
	})

	return
}

func (g *cursorGraph) insertD1(u *ld.BlankNode, c *constraint, txn *badger.Txn) (err error) {
	variable, _ := g.getVariable(u)
	if variable.cs == nil {
		variable.cs = constraintSet{c}
	} else {
		variable.cs = append(variable.cs, c)
	}

	c.count, err = c.getCount(g.unary, g.binary, txn)
	if err != nil {
		return
	} else if c.count == 0 {
		return ErrEndOfSolutions
	}

	p := (c.place + 1) % 3
	v, w := c.values[p], c.values[(p+1)%3]
	c.prefix = assembleKey(TernaryPrefixes[p], true, v, w)

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
	if variable.edges == nil {
		variable.edges = constraintMap{}
	}

	_, j := g.getVariable(v)
	if cs, has := variable.edges[j]; has {
		variable.edges[j] = append(cs, c)
	} else {
		variable.edges[j] = constraintSet{c}
	}

	if variable.cs == nil {
		variable.cs = constraintSet{c}
	} else {
		variable.cs = append(variable.cs, c)
	}

	c.count, err = c.getCount(g.unary, g.binary, txn)
	if err != nil {
		return
	} else if c.count == 0 {
		return ErrEndOfSolutions
	}

	var p Permutation
	if c.values[(c.place+1)%3] == "" {
		p = (c.place + 2) % 3
	} else if c.values[(c.place+2)%3] == "" {
		p = ((c.place + 1) % 3) + 3
	}

	c.prefix = assembleKey(BinaryPrefixes[p], true, c.values[p%3])

	// Create a new badger.Iterator for the constraint
	c.iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.prefix,
	})

	return
}
