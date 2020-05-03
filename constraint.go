package styx

import (
	"bytes"
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	rdf "github.com/underlay/go-rdfjs"
)

// A constraint to an occurrence of a variable in a query
type constraint struct {
	index     int         // The index of the triple within the query
	place     Permutation // The term (subject = 0, predicate = 1, object = 2) within the triple
	count     uint32      // The number of unique triples that satisfy the constraint
	prefix    []byte
	iterator  *badger.Iterator
	quad      *rdf.Quad
	terms     [3]ID
	neighbors []*constraint
}

// cache is a struct for holding cached value states
type cache = struct {
	i int
	j int
	c uint32
}

func (c *constraint) save(i, j int) cache {
	return cache{i, j, c.count}
}

func (c *constraint) print(p Permutation) string {
	t := c.quad[p].TermType()
	if t == rdf.BlankNodeType || t == rdf.VariableType {
		return fmt.Sprintf(" = %s", c.terms[p])
	}
	return c.quad[p].String()
}

func (c *constraint) Sources(value ID, txn *badger.Txn) (statements []*Statement, err error) {
	var item *badger.Item
	if c.place == 0 {
		item = c.iterator.Item()
	} else {
		c.terms[c.place] = value
		s, p, o := c.terms[0], c.terms[1], c.terms[2]
		key := assembleKey(TernaryPrefixes[0], false, s, p, o)
		item, err = txn.Get(key)
		if err != nil {
			return
		}
	}

	err = item.Value(func(val []byte) (err error) {
		statements, err = getStatements(val)
		return
	})

	return
}

func (c *constraint) String() string {
	if len(c.prefix) == 9 {
		return fmt.Sprintf(
			"(p%d {%s | %s} %s:%02d#%d)",
			c.place,
			c.print((c.place+1)%3), c.print((c.place+2)%3),
			string(c.prefix[0]),
			binary.BigEndian.Uint64(c.prefix[1:]),
			c.count,
		)
	} else if len(c.prefix) == 17 {
		return fmt.Sprintf(
			"(p%d {%s | %s} %s:%02d:%02d#%d)",
			c.place,
			c.print((c.place+1)%3), c.print((c.place+2)%3),
			string(c.prefix[0]),
			binary.BigEndian.Uint64(c.prefix[1:9]),
			binary.BigEndian.Uint64(c.prefix[9:]),
			c.count,
		)
	} else {
		return "<<<invalid constraint>>>"
	}
}

// Close the constraint's iterator, if it exists
func (c *constraint) Close() {
	if c.iterator != nil {
		c.iterator.Close()
		c.iterator = nil
	}
}

func (c *constraint) value() (v ID) {
	if c.iterator.ValidForPrefix(c.prefix) {
		item := c.iterator.Item()
		key := item.KeyCopy(nil)
		i := bytes.LastIndexByte(key, '\t')
		if i == -1 {
			i = 0
		}
		v = ID(key[i+1:])
	}

	return
}

// Next advances the iterator and returns the next value
func (c *constraint) Next() ID {
	c.iterator.Next()
	return c.value()
}

// Seek advances the iterator to the first value equal to
// or greater than given byte slice.
func (c *constraint) Seek(v ID) ID {
	key := make([]byte, len(c.prefix)+len(v))
	copy(key, c.prefix)
	if v != NIL {
		copy(key[len(c.prefix):], v)
	}
	c.iterator.Seek(key)
	return c.value()
}

func (c *constraint) getCount(uc unaryCache, bc binaryCache, txn *badger.Txn) (uint32, error) {
	j, k := (c.place+1)%3, (c.place+2)%3
	v, w := c.terms[j], c.terms[k]
	if v == NIL && w == NIL {
		// AAAA return the total number of variables??
		return 48329, nil
	} else if v == NIL {
		return uc.Get(k, w, txn)
	} else if w == NIL {
		return uc.Get(j+3, v, txn)
	} else {
		return bc.Get(j, v, w, txn)
	}
}

// A constraintSet is just a slice of Constraints.
type constraintSet []*constraint

// Close just calls Close on its child constraints
func (cs constraintSet) Close() {
	if cs != nil {
		for _, c := range cs {
			c.Close()
		}
	}
}

func (cs constraintSet) String() string {
	s := "[ "
	for i, c := range cs {
		if i > 0 {
			s += ", "
		}
		s += c.String()
	}
	return s + " ]"
}

// Sort interface for ConstraintSet
func (cs constraintSet) Len() int           { return len([]*constraint(cs)) }
func (cs constraintSet) Swap(a, b int)      { cs[a], cs[b] = cs[b], cs[a] }
func (cs constraintSet) Less(a, b int) bool { return cs[a].count < cs[b].count }

// Seek to the next intersection
func (cs constraintSet) Seek(v ID) ID {
	var count int
	l := cs.Len()
	for i := 0; count < l; i = (i + 1) % l {
		c := cs[i]
		next := c.Seek(v)
		if next == NIL {
			return NIL
		} else if next == v {
			count++
		} else {
			count = 1
			v = next
		}
	}
	return v
}

// Next value (could be improved to not double-check the first constraint)
func (cs constraintSet) Next() (next ID) {
	c := cs[0]
	c.iterator.Next()
	next = c.value()
	if next != NIL && len(cs) > 1 {
		next = cs.Seek(next)
	}
	return
}

// A constraintMap is a map of string variable labels to constraint sets.
type constraintMap map[int]constraintSet

// Len returns the total number of constraints in the constraint map
func (cm constraintMap) Len() (l int) {
	for _, cs := range cm {
		l += cs.Len()
	}
	return
}
