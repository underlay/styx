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
	graph     [][]Value
	constants []*constraint
	variables []*variable
	domain    []*ld.BlankNode
	pivot     int
	ids       map[string]int
	cache     []*vcache
	blacklist []bool
	in        [][]int
	out       [][]int
	values    valueCache
	binary    binaryCache
	unary     unaryCache
	txn       *badger.Txn
}

// Collect calls Next(nil) on the iterator until there are no more solutions,
// and returns all the results in a slice.
func (iter *Iterator) Collect() [][]ld.Node {
	result := [][]ld.Node{}
	var err error
	for d := iter.domain; err == nil; d, err = iter.Next(nil) {
		index := make([]ld.Node, len(d))
		for i, b := range d {
			index[i] = iter.Get(b)
		}
		result = append(result, index)
	}
	return result
}

// Log pretty-prints the contents of the database
func (iter *Iterator) Log() {
	domain := iter.Domain()
	attributes := make([]string, len(domain))
	for i, node := range domain {
		attributes[i] = node.Attribute
	}
	log.Println(strings.Join(attributes, "\t"))
	for _, path := range iter.Collect() {
		values := make([]string, len(domain))
		start := len(domain) - len(path)
		for i, node := range path {
			values[start+i] = node.GetValue()
		}
		log.Println(strings.Join(values, "\t"))
	}
}

// Graph returns a []*ld.Quad representation of the iterator's current value
func (iter *Iterator) Graph() []*ld.Quad {
	graph := make([]*ld.Quad, len(iter.graph))
	for i, quad := range iter.graph {
		graph[i] = ld.NewQuad(
			quad[0].Node("", iter.values, iter.txn),
			quad[1].Node("", iter.values, iter.txn),
			quad[2].Node("", iter.values, iter.txn),
			"",
		)
	}
	return graph
}

// Get the value for a particular blank node
func (iter *Iterator) Get(node *ld.BlankNode) (n ld.Node) {
	if node == nil {
		return
	}

	i, has := iter.ids[node.Attribute]
	if !has {
		return
	}

	v := iter.variables[i]
	if v.value == "" {
		return
	}

	value, _ := readValue([]byte(v.value))
	return value.Node("", iter.values, iter.txn)
}

// Domain returns the total ordering of variables used by the iterator
func (iter *Iterator) Domain() []*ld.BlankNode {
	domain := make([]*ld.BlankNode, len(iter.domain))
	copy(domain, iter.domain)
	return domain
}

// Index returns the iterator's current value as an ordered slice of ld.Nodes
func (iter *Iterator) Index() []ld.Node {
	index := make([]ld.Node, len(iter.variables))
	for i, v := range iter.variables {
		value, _ := readValue([]byte(v.value))
		index[i] = value.Node("", iter.values, iter.txn)
	}
	return index
}

// Next advances the iterator to the next result that differs in the given node.
// If nil is passed, the last node in the domain is used.
func (iter *Iterator) Next(node *ld.BlankNode) ([]*ld.BlankNode, error) {
	i := iter.Len() - 1
	if node != nil {
		i = iter.ids[node.Attribute]
	}
	tail, err := iter.next(i)
	if err == badger.ErrKeyNotFound {
		err = ErrEndOfSolutions
	}

	if err != nil {
		return nil, err
	}

	if tail == iter.Len() {
		return nil, ErrEndOfSolutions
	}
	return iter.domain[tail:], nil
}

// Close the iterator
func (iter *Iterator) Close() {
	if iter != nil {
		if iter.variables != nil {
			for _, u := range iter.variables {
				u.Close()
			}
		}
		if iter.txn != nil {
			iter.txn.Discard()
		}
	}
}

func (iter *Iterator) String() string {
	s := "----- Constraint Graph -----\n"
	for i, id := range iter.domain {
		s += fmt.Sprintf("---- %s ----\n%s\n", id, iter.variables[i].String())
	}
	s += fmt.Sprintln("----- End of Constraint Graph -----")
	return s
}

// Sort interface functions
func (iter *Iterator) Len() int { return len(iter.domain) }
func (iter *Iterator) Swap(a, b int) {
	// Swap is very important in assembling the graph.
	// The things it _does_ mutate are .variables and .domain.
	// It does NOT mutate .ids or the constraints, which is how we
	/// construct the transformation maps.
	if iter.pivot < a && iter.pivot < b {
		iter.variables[a], iter.variables[b] = iter.variables[b], iter.variables[a]
		iter.domain[a], iter.domain[b] = iter.domain[b], iter.domain[a]
	}
}

// TODO: put more thought into the sorting heuristic.
// Right now the variables are sorted their norm: in
// increasing order of their length-normalized sum of
// the squares of the counts of all their constraints (of any degree).
func (iter *Iterator) Less(a, b int) bool {
	A, B := iter.variables[a], iter.variables[b]
	return (float32(A.norm) / float32(A.size)) < (float32(B.norm) / float32(B.size))
}

func (iter *Iterator) insertDZ(u *variable, c *constraint, txn *badger.Txn) (err error) {
	if u.cs == nil {
		u.cs = constraintSet{c}
	} else {
		u.cs = append(u.cs, c)
	}

	c.count, err = c.getCount(iter.unary, iter.binary, txn)
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

func (iter *Iterator) insertD1(u *variable, c *constraint, txn *badger.Txn) (err error) {
	if u.cs == nil {
		u.cs = constraintSet{c}
	} else {
		u.cs = append(u.cs, c)
	}

	c.count, err = c.getCount(iter.unary, iter.binary, txn)
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

func (iter *Iterator) insertD2(u, v *variable, c *constraint, txn *badger.Txn) (err error) {
	// For second-degree constraints we get the *count* with an index key
	// and set the *prefix* to either a major or minor key

	if u.edges == nil {
		u.edges = constraintMap{}
	}

	j := iter.getIndex(v)
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

	c.count, err = c.getCount(iter.unary, iter.binary, txn)
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

func (iter *Iterator) getIndex(u *variable) int {
	for i, v := range iter.variables {
		if u == v {
			return i
		}
	}
	log.Fatalln("Invalid variable index")
	return -1
}
