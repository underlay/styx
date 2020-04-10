package styx

import (
	"fmt"
	"log"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// An Iterator exposes Next and Seek operations
type Iterator struct {
	constants []*constraint
	variables []*variable
	domain    []*ld.BlankNode
	pivot     int
	ids       map[string]int
	cache     []*V
	blacklist []bool
	in        [][]int
	out       [][]int
	values    valueCache
	binary    binaryCache
	unary     unaryCache
	txn       *badger.Txn
}

func (g *Iterator) Collect() [][]ld.Node {
	result := [][]ld.Node{}
	var err error
	for d := g.domain; err == nil; d, err = g.Next(nil) {
		index := make([]ld.Node, len(d))
		for i, b := range d {
			index[i] = g.Get(b)
		}
		result = append(result, index)
	}
	return result
}

func (g *Iterator) Log() {
	domain := g.Domain()
	attributes := make([]string, len(domain))
	for i, node := range domain {
		attributes[i] = node.Attribute
	}
	log.Println(strings.Join(attributes, "\t"))
	for _, path := range g.Collect() {
		values := make([]string, len(domain))
		start := len(domain) - len(path)
		for i, node := range path {
			values[start+i] = node.GetValue()
		}
		log.Println(strings.Join(values, "\t"))
	}
}

func (g *Iterator) Graph() []*ld.Quad {
	return nil
}

func (g *Iterator) Get(node *ld.BlankNode) (n ld.Node) {
	if node == nil {
		return
	}

	i, has := g.ids[node.Attribute]
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

func (g *Iterator) Domain() []*ld.BlankNode {
	domain := make([]*ld.BlankNode, len(g.domain))
	copy(domain, g.domain)
	return domain
}

func (g *Iterator) Index() []ld.Node {
	index := make([]ld.Node, len(g.variables))
	for i, v := range g.variables {
		value, _ := readValue([]byte(v.value))
		index[i] = value.Node("", g.values, g.txn)
	}
	return index
}

func (g *Iterator) Next(node *ld.BlankNode) ([]*ld.BlankNode, error) {
	i := g.Len() - 1
	if node != nil {
		i = g.ids[node.Attribute]
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

func (g *Iterator) Seek(index []ld.Node) error {
	return nil
}

func (g *Iterator) Close() {
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

func (g *Iterator) String() string {
	s := "----- Constraint Graph -----\n"
	for i, id := range g.domain {
		s += fmt.Sprintf("---- %s ----\n%s\n", id, g.variables[i].String())
	}
	s += fmt.Sprintln("----- End of Constraint Graph -----")
	return s
}

// Sort interface functions
func (g *Iterator) Len() int { return len(g.domain) }
func (g *Iterator) Swap(a, b int) {
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
func (g *Iterator) Less(a, b int) bool {
	A, B := g.variables[a], g.variables[b]
	return (float32(A.norm) / float32(A.size)) < (float32(B.norm) / float32(B.size))
}

func (g *Iterator) insertDZ(u *variable, c *constraint, txn *badger.Txn) (err error) {
	if u.cs == nil {
		u.cs = constraintSet{c}
	} else {
		u.cs = append(u.cs, c)
	}

	c.count, err = c.getCount(g.unary, g.binary, txn)
	if err != nil {
		return
	} else if c.count == 0 {
		return ErrEndOfSolutions
	}

	p := (c.place + 2) % 3
	c.prefix = assembleKey(BinaryPrefixes[p], true, c.terms[p])

	// Create a new badger.Iterator for the constraint
	c.iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.prefix,
	})

	return
}

func (g *Iterator) insertD1(u *variable, c *constraint, txn *badger.Txn) (err error) {
	if u.cs == nil {
		u.cs = constraintSet{c}
	} else {
		u.cs = append(u.cs, c)
	}

	c.count, err = c.getCount(g.unary, g.binary, txn)
	if err != nil {
		return
	} else if c.count == 0 {
		return ErrEndOfSolutions
	}

	p := (c.place + 1) % 3
	v, w := c.terms[p], c.terms[(p+1)%3]
	c.prefix = assembleKey(TernaryPrefixes[p], true, v, w)

	// Create a new badger.Iterator for the constraint
	c.iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.prefix,
	})

	return
}

func (g *Iterator) getIndex(u *variable) int {
	for i, v := range g.variables {
		if u == v {
			return i
		}
	}
	log.Fatalln("Invalid variable index")
	return -1
}

func (g *Iterator) insertD2(u, v *variable, c *constraint, txn *badger.Txn) (err error) {
	// For second-degree constraints we get the *count* with an index key
	// and set the *prefix* to either a major or minor key

	if u.edges == nil {
		u.edges = constraintMap{}
	}

	j := g.getIndex(v)
	if cs, has := u.edges[j]; has {
		u.edges[j] = append(cs, c)
	} else {
		u.edges[j] = constraintSet{c}
	}

	if u.cs == nil {
		u.cs = constraintSet{c}
	} else {
		u.cs = append(u.cs, c)
	}

	c.count, err = c.getCount(g.unary, g.binary, txn)
	if err != nil {
		return
	} else if c.count == 0 {
		return ErrEndOfSolutions
	}

	var p Permutation
	if c.terms[(c.place+1)%3] == "" {
		p = (c.place + 2) % 3
	} else if c.terms[(c.place+2)%3] == "" {
		p = ((c.place + 1) % 3) + 3
	}

	c.prefix = assembleKey(BinaryPrefixes[p], true, c.terms[p%3])

	// Create a new badger.Iterator for the constraint
	c.iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.prefix,
	})

	return
}
