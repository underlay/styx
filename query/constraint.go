package query

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger"
	proto "github.com/gogo/protobuf/proto"

	types "github.com/underlay/styx/types"
)

// A Generator generates, that's what it does
type Generator interface {
	String() string
	Close()
	Next() []byte
	Seek(v []byte) []byte
}

// A Constraint to an occurrence of a variable in a dataset
type Constraint struct {
	Index    int
	Place    uint8  // The term (subject/predicate/object) within the triple
	M        HasID  // The next (clockwise) element in the triple
	m        []byte // a convience slot for the []byte of M, if it exists
	N        HasID  // The previous element in the triple
	n        []byte // a convience slot for the []byte of N, if it exists
	Count    uint64
	Prefix   []byte
	Iterator *badger.Iterator
	Dual     *Constraint // If (M or N) is a blank node, this is a pointer to the mirror struct
}

func (c *Constraint) printM() (s string) {
	if b, is := c.M.(BlankNode); is {
		s = string(b)
		if c.m != nil {
			s += fmt.Sprintf(" = %02d", binary.BigEndian.Uint64(c.m))
		}
	} else if i, is := c.M.(*types.Index); is {
		s = i.String()
	}
	return
}

func (c *Constraint) printN() (s string) {
	if b, is := c.N.(BlankNode); is {
		s = string(b)
		if c.n != nil {
			s += fmt.Sprintf(" = %02d", binary.BigEndian.Uint64(c.n))
		}
	} else if i, is := c.N.(*types.Index); is {
		s = i.String()
	}
	return
}

// Sources can only be called on a first-degree constraint
// and it returns the unmarshalled SourceList from the value
// of the badger iterator's current item
func (c *Constraint) Sources() (*types.SourceList, error) {
	sources := &types.SourceList{}
	item := c.Iterator.Item()
	if val, err := item.ValueCopy(nil); err != nil {
		return nil, err
	} else {
		return sources, proto.Unmarshal(val, sources)
	}
}

func (c *Constraint) String() string {
	if len(c.Prefix) == 9 {
		return fmt.Sprintf(
			"(p%d {%s | %s} %s:%02d#%d)",
			c.Place,
			c.printM(), c.printN(),
			string(c.Prefix[0]),
			binary.BigEndian.Uint64(c.Prefix[1:]),
			c.Count,
		)
	} else if len(c.Prefix) == 17 {
		return fmt.Sprintf(
			"(p%d {%s | %s} %s:%02d:%02d#%d)",
			c.Place,
			c.printM(), c.printN(),
			string(c.Prefix[0]),
			binary.BigEndian.Uint64(c.Prefix[1:9]),
			binary.BigEndian.Uint64(c.Prefix[9:]),
			c.Count,
		)
	} else {
		return "<<<invalid constraint>>>"
	}
}

// Close the constraint's cursor's iterator, if it exists
func (c *Constraint) Close() {
	if c.Iterator != nil {
		c.Iterator.Close()
		c.Iterator = nil
	}
}

func (c *Constraint) value() (v []byte) {
	item := c.Iterator.Item()
	key := item.KeyCopy(make([]byte, len(c.Prefix)+8))
	if c.Iterator.ValidForPrefix(c.Prefix) {
		prefix := key[0]
		if _, has := types.TriplePrefixMap[prefix]; has {
			v = key[17:25]
		} else if _, has := types.MajorPrefixMap[prefix]; has {
			v = key[9:17]
		} else if _, has := types.MinorPrefixMap[prefix]; has {
			v = key[9:17]
		} else {
			v = key[1:9] // Should never happen?
		}
	}

	return
}

// Next advances the iterator and returns the next value
func (c *Constraint) Next() []byte {
	c.Iterator.Next()
	return c.value()
}

// Seek advances the cursor to the first value equal to
// or greater than given byte slice.
func (c *Constraint) Seek(v []byte) []byte {
	key := make([]byte, len(c.Prefix)+8)
	copy(key[:len(c.Prefix)], c.Prefix)
	if v != nil {
		copy(key[len(c.Prefix):], v)
	}
	c.Iterator.Seek(key)
	return c.value()
}

// Set the value of a temporary assignment
func (c *Constraint) Set(v []byte, txn *badger.Txn) (err error) {
	place := (c.Place + 1) % 3
	prefix := types.TriplePrefixes[place]

	if _, is := c.M.(BlankNode); is {
		c.Prefix = types.AssembleKey(prefix, v, c.n, nil)
		c.m = v
	} else if _, is := c.N.(BlankNode); is {
		c.Prefix = types.AssembleKey(prefix, c.m, v, nil)
		c.n = v
	}

	// This call to getCount could theoretically be eliminated if we retrieve
	// and store the *values* of second-degree constraint keys during seeking.
	c.Count, err = c.getCount(txn)

	return
}

func (c *Constraint) getCount(txn *badger.Txn) (count uint64, err error) {
	// ONLY call getCount on first-degree constraints _or_ second-degree constraints
	// whose other variable has been temporarily set!!
	if c.n == nil || c.m == nil {
		return
	}

	// Assemble a major key using the two constant values m and n
	key := types.AssembleKey(types.MajorPrefixes[(c.Place+1)%3], c.m, c.n, nil)
	// **We could equally as easily have assembled a minor key to get the count**
	// key = types.AssembleKey(types.MinorPrefixes[(c.Place+2)%3], c.n, c.m, nil)

	var item *badger.Item
	val := make([]byte, 8)
	if item, err = txn.Get(key); err == badger.ErrKeyNotFound {
		return 0, nil
	} else if err != nil {
		return
	} else if val, err = item.ValueCopy(val); err != nil {
		return
	}
	count = binary.BigEndian.Uint64(val)
	return
}

// A ConstraintSet is just a slice of Constraints.
type ConstraintSet []*Constraint

// GeneratorSet is fjksdlfjksfjksdl
type GeneratorSet interface {
	sort.Interface
	String() string
	Close()
	Seek(v []byte) ([]byte, int)
	Next() []byte
}

// Close just calls Close on its child constraints
func (cs ConstraintSet) Close() {
	for _, c := range cs {
		c.Close()
	}
}

func (cs ConstraintSet) String() string {
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
func (cs ConstraintSet) Len() int           { return len(cs) }
func (cs ConstraintSet) Swap(a, b int)      { cs[a], cs[b] = cs[b], cs[a] }
func (cs ConstraintSet) Less(a, b int) bool { return cs[a].Count < cs[b].Count }

// Seek to the next intersection
func (cs ConstraintSet) Seek(v []byte) ([]byte, int) {
	var count int
	l := len(cs)
	for i := 0; count < l; i = (i + 1) % l {
		c := cs[i]
		next := c.Seek(v)
		if next == nil {
			return nil, 0
		} else if bytes.Equal(next, v) {
			count++
		} else {
			count = 1
			v = next
		}
	}
	return v, count
}

// Next value (could be improved to not double-check cursor[0])
func (cs ConstraintSet) Next() (next []byte) {
	c := cs[0]
	c.Iterator.Next()
	if next = c.value(); next != nil {
		next, _ = cs.Seek(next)
	}
	return
}

// A ConstraintMap is a map of string variable labels to constraint sets.
type ConstraintMap map[string]ConstraintSet

// A ConstraintGraph associates ids with Variable maps.
type ConstraintGraph struct {
	Index map[string]*Variable
	Slice []string
	Pivot int
	Root  map[string][]byte
	Map   map[string]int
	In    map[string][]int
	Out   map[string][]int
	Rules []*types.Rule
}

func (g *ConstraintGraph) String() string {
	s := "----- Constraint Graph -----\n"
	for _, id := range g.Slice {
		s += fmt.Sprintf("---- %s ----\n%s\n", id, g.Index[id].String())
	}
	s += fmt.Sprintln("----- End of Constraint Graph -----")
	return s
}

// Close just calls Close on its child constraints
func (g *ConstraintGraph) Close() {
	if g != nil && g.Slice != nil && g.Index != nil {
		for _, id := range g.Slice {
			if index, has := g.Index[id]; has {
				index.Close()
			}
		}
	}
}

// Sort interface functions
func (g *ConstraintGraph) Len() int { return len(g.Slice) }
func (g *ConstraintGraph) Swap(a, b int) {
	g.Slice[a], g.Slice[b] = g.Slice[b], g.Slice[a]
}

// TODO: put more thought into the sorting heuristic.
// Right now the variables are sorted their norm: in
// increasing order of their length-normalized sum of
// the squares of the counts of all their constraints (of any degree).
func (g *ConstraintGraph) Less(a, b int) bool {
	A, B := g.Index[g.Slice[a]], g.Index[g.Slice[b]]
	return (float32(A.Norm) / float32(A.Size)) < (float32(B.Norm) / float32(B.Size))
}

// Get retrieves an Variable or creates one if it doesn't exist.
func (g *ConstraintGraph) Get(id string) *Variable {
	if g.Index == nil {
		g.Index = map[string]*Variable{}
	}
	v, has := g.Index[id]
	if !has {
		v = &Variable{}
		g.Index[id] = v
		g.Slice = append(g.Slice, id)
	}
	return v
}

// GetIndex is a convenience method for retrieving a variable by its integer index
func (g *ConstraintGraph) GetIndex(i int) (string, *Variable) {
	p := g.Slice[i]
	return p, g.Index[p]
}

func (g *ConstraintGraph) insertDZ(u string, c *Constraint, txn *badger.Txn) (err error) {
	// For z-degree constraints we get the *count* with an index key
	// and set the *prefix* to a major key (although we could also use a minor key)

	variable := g.Get(u)
	if variable.DZ == nil {
		variable.DZ = ConstraintSet{c}
	} else {
		variable.DZ = append(variable.DZ, c)
	}

	place := (c.Place + 2) % 3

	c.Prefix = types.AssembleKey(types.MajorPrefixes[place], c.n, nil, nil)

	if c.Count = c.N.(*types.Index).Get(place); c.Count == 0 {
		return ErrInitialCountZero
	}

	// Create a new badger.Iterator for the constraint
	c.Iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.Prefix,
	})

	return
}

func (g *ConstraintGraph) insertD1(u string, c *Constraint, txn *badger.Txn) (err error) {
	// For first-degree constraints we get the *count* with a major key
	// and set the *prefix* to a triple key

	variable := g.Get(u)
	if variable.D1 == nil {
		variable.D1 = ConstraintSet{c}
	} else {
		variable.D1 = append(variable.D1, c)
	}

	// We rotate forward to get a major key, or backward to get a minor key.
	place := (c.Place + 1) % 3
	c.Prefix = types.AssembleKey(types.TriplePrefixes[place], c.m, c.n, nil)

	if c.Count, err = c.getCount(txn); err != nil {
		return
	} else if c.Count == 0 {
		return ErrInitialCountZero
	}

	// Create a new badger.Iterator for the constraint
	c.Iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.Prefix,
	})

	return
}

func (g *ConstraintGraph) insertD2(u string, v string, c *Constraint, txn *badger.Txn) (err error) {
	// For second-degree constraints we get the *count* with an index key
	// and set the *prefix* to either a major or minor key

	variable := g.Get(u)
	if variable.D2 == nil {
		variable.D2 = ConstraintMap{}
	}
	if cs, has := variable.D2[v]; has {
		variable.D2[v] = append(cs, c)
	} else {
		variable.D2[v] = ConstraintSet{c}
	}

	if index, is := c.M.(*types.Index); is {
		place := (c.Place + 1) % 3
		c.Count = index.Get(place)
		c.Prefix = types.AssembleKey(types.MinorPrefixes[place], c.m, nil, nil)
	} else if index, is := c.N.(*types.Index); is {
		place := (c.Place + 2) % 3
		c.Count = index.Get(place)
		c.Prefix = types.AssembleKey(types.MajorPrefixes[place], c.n, nil, nil)
	}

	if c.Count == 0 {
		return ErrInitialCountZero
	}

	// Create a new badger.Iterator for the constraint
	c.Iterator = txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         c.Prefix,
	})

	return
}
