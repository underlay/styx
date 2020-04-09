package styx

import (
	"fmt"
	"sort"
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

// V is a struct that caches a variable's total state
type V struct {
	Term
	caches
}

func (u *variable) save() *V {
	d := make(caches, 0, u.edges.Len())
	for q, cs := range u.edges {
		for i, c := range cs {
			d = append(d, c.save(q, i))
		}
	}
	return &V{u.value, d}
}

func (g *cursorGraph) load(u *variable, v *V) {
	u.value = v.Term
	for _, d := range v.caches {
		c := u.edges[d.i][d.j]

		node := variableNode(g.domain[d.i].Attribute)
		m, n := (c.place+1)%3, (c.place+2)%3

		p, other := c.place, c.place
		if node == c.nodes[m] {
			c.values[m], p, other = v.Term, m+3, n
		} else if node == c.nodes[n] {
			c.values[n], p, other = v.Term, n, m
		}

		if c.values[other] == "" {
			c.prefix = assembleKey(BinaryPrefixes[p], true, v.Term)
		} else {
			c.prefix = assembleKey(TernaryPrefixes[m], true, c.values[m], c.values[n])
		}

		c.count = d.c
		c.Seek(u.value)
	}
}
