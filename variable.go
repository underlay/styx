package styx

import (
	"fmt"
	"sort"

	rdf "github.com/underlay/go-rdfjs"
)

// variable is a collection of constraints on a particular blank node.
type variable struct {
	node  rdf.Term
	cs    constraintSet // Static and incoming constraints
	edges constraintMap // Outgoing constraints
	value ID            // Tha val
	root  ID            // the first possible value for the variable, without joining on other variables
	norm  uint64        // The sum of squares of key counts of constraints
	score float64       // norm / size
}

func (u *variable) ID() ID {
	return u.value
}

func (u *variable) String() (s string) {
	if u.value != NIL {
		s += fmt.Sprintf("Value: %s\n", u.value)
	}
	if u.root != NIL {
		s += fmt.Sprintf("Root: %s\n", u.root)
	}
	// s += fmt.Sprintf("DZ: %s\n", u.DZ.String())
	// s += fmt.Sprintf("D1: %s\n", u.D1.String())
	s += fmt.Sprintln("D2:")
	for id, cs := range u.edges {
		s += fmt.Sprintf("  %d: %s\n", id, cs.String())
	}
	s += fmt.Sprintf("Norm: %d\n", u.norm)
	s += fmt.Sprintf("Size: %d\n", u.cs.Len())
	s += fmt.Sprintf("Score: %f\n", u.score)
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
func (u *variable) Seek(value ID) ID {
	return u.cs.Seek(value)
}

// Next returns the next intersect value
func (u *variable) Next() ID {
	return u.cs.Next()
}

// caches is a slice of C structs
type caches []cache

// vcache is a struct that caches a variable's total state
type vcache struct {
	ID
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

func (g *Iterator) load(u *variable, vc *vcache) {
	u.value = vc.ID
	for _, d := range vc.caches {
		c := u.edges[d.i][d.j]

		v := g.variables[d.i]
		m, n := (c.place+1)%3, (c.place+2)%3

		p, other := c.place, c.place
		if v.node.Equal(c.quad[m]) {
			c.terms[m], p, other = vc.ID, m+3, n
		} else if v.node.Equal(c.quad[n]) {
			c.terms[n], p, other = vc.ID, n, m
		}

		if c.terms[other] == NIL {
			c.prefix = assembleKey(BinaryPrefixes[p], true, vc.ID)
		} else {
			c.prefix = assembleKey(TernaryPrefixes[m], true, c.terms[m], c.terms[n])
		}

		c.count = d.c
		c.Seek(u.value)
	}
}
