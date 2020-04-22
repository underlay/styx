package styx

import (
	"fmt"
	"log"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	rdf "github.com/underlay/go-rdfjs"
)

// An Iterator exposes Next and Seek operations
type Iterator struct {
	query      []*rdf.Quad
	constants  []*constraint
	variables  []*variable
	domain     []rdf.Term
	pivot      int
	bot        bool
	top        bool
	empty      bool
	ids        map[string]int
	cache      []*vcache
	blacklist  []bool
	in         [][]int
	out        [][]int
	binary     binaryCache
	unary      unaryCache
	tag        TagScheme
	txn        *badger.Txn
	dictionary Dictionary
}

// Collect calls Next(nil) on the iterator until there are no more solutions,
// and returns all the results in a slice.
func (iter *Iterator) Collect() ([][]rdf.Term, error) {
	if iter.empty {
		return nil, nil
	}

	result := [][]rdf.Term{}
	for d, err := iter.Next(nil); d != nil; d, err = iter.Next(nil) {
		if err != nil {
			return nil, err
		}

		index := make([]rdf.Term, len(d))
		for i, b := range d {
			index[i] = iter.Get(b)
		}
		result = append(result, index)
	}
	return result, nil
}

// Log pretty-prints the iterator
func (iter *Iterator) Log() {
	if iter.empty {
		return
	}

	domain := iter.Domain()
	values := make([]string, len(domain))
	for i, node := range domain {
		values[i] = node.String()
	}
	log.Println(strings.Join(values, "\t"))

	for d, err := iter.Next(nil); d != nil; d, err = iter.Next(nil) {
		if err != nil {
			return
		}

		values := make([]string, len(domain))
		start := len(domain) - len(d)
		for i, node := range d {
			values[start+i] = node.String()
		}
		log.Println(strings.Join(values, "\t"))
	}
}

// Graph returns a []*rdfjs.Quad representation of the iterator's current value
func (iter *Iterator) Graph() []*rdf.Quad {
	if iter.empty {
		return nil
	}

	graph := make([]*rdf.Quad, len(iter.query))
	for i, quad := range iter.query {
		graph[i] = rdf.NewQuad(
			iter.variate(quad[0]),
			iter.variate(quad[1]),
			iter.variate(quad[2]),
			rdf.Default,
		)
	}
	return graph
}

func (iter *Iterator) variate(term rdf.Term) rdf.Term {
	switch term := term.(type) {
	case *rdf.Variable:
		return iter.Get(term)
	case *rdf.BlankNode:
		return iter.Get(term)
	default:
		return term
	}
}

// Get the value for a particular blank node
func (iter *Iterator) Get(node rdf.Term) rdf.Term {
	if iter.empty || node == nil {
		return nil
	}

	var value string
	switch node := node.(type) {
	case *rdf.Variable:
		value = node.String()
	case *rdf.BlankNode:
		value = node.String()
	default:
		return node
	}

	i, has := iter.ids[value]
	if !has {
		return nil
	}

	v := iter.variables[i]
	if v.value == NIL {
		return nil
	}

	n, _ := iter.dictionary.GetTerm(v.value, rdf.Default)
	return n
}

// Domain returns the total ordering of variables used by the iterator
func (iter *Iterator) Domain() []rdf.Term {
	if iter.empty {
		return nil
	}

	domain := make([]rdf.Term, len(iter.domain))
	copy(domain, iter.domain)
	return domain
}

// Index returns the iterator's current value as an ordered slice of ld.Nodes
func (iter *Iterator) Index() []rdf.Term {
	if iter.empty {
		return nil
	}

	index := make([]rdf.Term, len(iter.variables))
	for i, v := range iter.variables {
		index[i], _ = iter.dictionary.GetTerm(v.value, rdf.Default)
	}
	return index
}

// Next advances the iterator to the next result that differs in the given node.
// If nil is passed, the last node in the domain is used.
func (iter *Iterator) Next(node rdf.Term) ([]rdf.Term, error) {
	if iter.top || iter.empty {
		return nil, nil
	}

	if iter.bot {
		iter.bot = false
		return iter.Index(), nil
	}

	i := iter.pivot - 1
	if node != nil {
		value := node.String()
		index, has := iter.ids[value]
		if has {
			i = index
		}
	} else if iter.pivot == 0 {
		return nil, nil
	}

	tail, err := iter.next(i)
	if err != nil {
		return nil, err
	}

	l := iter.Len()
	if tail == l {
		iter.top = true
		return nil, nil
	}

	result := make([]rdf.Term, l-tail)
	for i, u := range iter.variables[tail:] {
		result[i], _ = iter.dictionary.GetTerm(u.value, rdf.Default)
	}

	return result, nil
}

// Seek advances the iterator to the first result
// greater than or equal to the given index path
func (iter *Iterator) Seek(index []rdf.Term) (err error) {
	if iter.empty {
		return
	}

	iter.bot = true
	iter.top = false

	terms := make([]ID, len(index))
	for i, node := range index {
		terms[i], err = iter.dictionary.GetID(node, rdf.Default)
		if err != nil {
			return err
		}
	}

	l := iter.Len()
	var ok bool

	for _, u := range iter.variables {
		u.value = u.root
	}

	for i, u := range iter.variables {
		var root ID
		if i < len(terms) && u.root < terms[i] {
			root = terms[i]
		} else if u.value == NIL {
			root = u.root
		}

		if root != NIL {
			for u.value = u.Seek(root); u.value == NIL; u.value = u.Seek(root) {
				ok, err = iter.tick(i, 0, iter.cache)
				if err != nil || !ok {
					return
				}
				if !ok {
					iter.top = true
					return
				}
			}
		}

		// We've got a non-nil value for u!
		err = iter.push(u, i, l)
		if err != nil {
			return err
		}
		for j, saved := range iter.cache[:i] {
			if saved != nil {
				iter.cache[j] = nil
				if i+1 < l {
					err = iter.push(iter.variables[j], i, l)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	return
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
		if iter.dictionary != nil {
			iter.dictionary.Commit()
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
	iter.variables[a], iter.variables[b] = iter.variables[b], iter.variables[a]
	iter.domain[a], iter.domain[b] = iter.domain[b], iter.domain[a]
}

// TODO: put more thought into the sorting heuristic.
// Right now the variables are sorted their norm: in
// increasing order of their length-normalized sum of
// the squares of the counts of all their constraints (of any degree).
func (iter *Iterator) Less(a, b int) bool {
	// So pivot right now is the length of the provided domain.
	// We keep those in order...
	if a < iter.pivot {
		return a < b
	} else if b < iter.pivot {
		return false
	}

	A, B := iter.variables[a], iter.variables[b]
	at, bt := A.node.TermType(), B.node.TermType()
	if at == rdf.VariableType && bt == rdf.BlankNodeType {
		return true
	} else if at == rdf.BlankNodeType && bt == rdf.VariableType {
		return false
	}

	return A.score < B.score
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
	if c.terms[(c.place+1)%3] == NIL {
		p = (c.place + 2) % 3
	} else if c.terms[(c.place+2)%3] == NIL {
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
