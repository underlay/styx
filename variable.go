package styx

import (
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// variable is a collection of constraints on a particular blank node.
type variable struct {
	cs    constraintSet // Static and incoming constraints
	edges constraintMap // Outgoing constraints
	value Term          // Tha val
	root  Term          // the first possible value for the variable, without joining on other variables
	size  int           // The total number of constraints
	norm  uint64        // The sum of squares of key counts of constraints
}

func (u *variable) Term() Term {
	return u.value
}

// JSON returns a JSON-LD value for the literal, satisfying the Value interface
func (u *variable) JSON(origin iri, cache valueCache, txn *badger.Txn) (r interface{}) {
	return
}

// NQuads returns the n-quads term for the literal, satisfying the Value interface
func (u *variable) NQuads(origin iri, cache valueCache, txn *badger.Txn) string {

	return ""
}

func (u *variable) Node(origin iri, cache valueCache, txn *badger.Txn) ld.Node {
	return nil
}

func (u *variable) String() (s string) {
	if u.value != "" {
		s += fmt.Sprintf("Value: %s\n", u.value)
	}
	if u.root != "" {
		s += fmt.Sprintf("Root: %s\n", u.root)
	}
	// s += fmt.Sprintf("DZ: %s\n", u.DZ.String())
	// s += fmt.Sprintf("D1: %s\n", u.D1.String())
	s += fmt.Sprintln("D2:")
	for id, cs := range u.edges {
		s += fmt.Sprintf("  %d: %s\n", id, cs.String())
	}
	s += fmt.Sprintf("Norm: %d\n", u.norm)
	s += fmt.Sprintf("Size: %d\n", u.size)
	return
}

// Close just calls Close on its child constraint sets
func (u *variable) Close() {
	if u != nil {
		u.cs.Close()
	}
}

// Sort the constraints by raw count
func (u *variable) Sort() {
	sort.Sort(u.cs)
}

// Seek to the next intersect value
func (u *variable) Seek(value Term) Term {
	return u.cs.Seek(value)
}

// Next returns the next intersect value
func (u *variable) Next() Term {
	return u.cs.Next()
}

// caches is a slice of C structs
type caches []cache

// vcache is a struct that caches a variable's total state
type vcache struct {
	Term
	caches
}

func (u *variable) save() *vcache {
	d := make(caches, 0, u.edges.Len())
	for q, cs := range u.edges {
		for i, c := range cs {
			d = append(d, c.save(q, i))
		}
	}
	return &vcache{u.value, d}
}

func (g *Iterator) load(u *variable, v *vcache) {
	u.value = v.Term
	for _, d := range v.caches {
		c := u.edges[d.i][d.j]

		node := g.variables[d.i]
		m, n := (c.place+1)%3, (c.place+2)%3

		p, other := c.place, c.place
		if node == c.values[m] {
			c.terms[m], p, other = v.Term, m+3, n
		} else if node == c.values[n] {
			c.terms[n], p, other = v.Term, n, m
		}

		if c.terms[other] == "" {
			c.prefix = assembleKey(BinaryPrefixes[p], true, v.Term)
		} else {
			c.prefix = assembleKey(TernaryPrefixes[m], true, c.terms[m], c.terms[n])
		}

		c.count = d.c
		c.Seek(u.value)
	}
}
