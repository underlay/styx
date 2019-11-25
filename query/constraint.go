package query

import (
	"bytes"
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	proto "github.com/gogo/protobuf/proto"
	types "github.com/underlay/styx/types"
)

// A Constraint to an occurrence of a variable in a dataset
type Constraint struct {
	Index    int    // The index of the triple within the dataset
	Place    uint8  // The term (subject = 0, predicate = 1, object = 2) within the triple
	M        HasID  // The next ((Place + 1) % 3) element in the triple
	m        I      // a convience slot for the []byte of M, if it is known at the time
	N        HasID  // The previous ((Place + 2) % 3) element in the triple
	n        I      // a convience slot for the []byte of N, if it is known at the time
	Count    uint64 // The number of triples that satisfy the Constraint
	Prefix   []byte // This is m and n (whichever exist) appended to the appropriate prefix
	Iterator *badger.Iterator
	Dual     *Constraint // If (M or N) is a blank node, this is a pointer to their dual struct
}

type C = struct {
	i int
	j int
	c uint64
}

func (c *Constraint) save(i, j int) C {
	return C{i, j, c.Count}
}

func (c *Constraint) printM() (s string) {
	if b, is := c.M.(VariableNode); is {
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
	if b, is := c.N.(VariableNode); is {
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
func (c *Constraint) Sources(value I, txn *badger.Txn) (statements []*types.Statement, err error) {
	var item *badger.Item
	if c.Place == 0 {
		item = c.Iterator.Item()
	} else {
		var s, p, o I
		if c.Place == 1 {
			s, p, o = c.n, value, c.m
		} else if c.Place == 2 {
			s, p, o = c.m, c.n, value
		}
		key := types.AssembleKey(types.TriplePrefixes[0], s, p, o)
		if item, err = txn.Get(key); err != nil {
			return
		}
	}

	sources := &types.SourceList{}
	var val []byte
	if val, err = item.ValueCopy(nil); err != nil {
		return
	} else if err = proto.Unmarshal(val, sources); err != nil {
		return
	} else {
		statements = sources.GetSources()
	}
	return
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

func (c *Constraint) value() (v I) {
	if c.Iterator.ValidForPrefix(c.Prefix) {
		item := c.Iterator.Item()
		key := item.KeyCopy(make([]byte, len(c.Prefix)+8))
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
func (c *Constraint) Next() I {
	c.Iterator.Next()
	return c.value()
}

// Seek advances the cursor to the first value equal to
// or greater than given byte slice.
func (c *Constraint) Seek(v I) I {
	key := make([]byte, len(c.Prefix)+8)
	copy(key[:len(c.Prefix)], c.Prefix)
	if v != nil {
		copy(key[len(c.Prefix):], v)
	}
	c.Iterator.Seek(key)
	return c.value()
}

// Set the value of a temporary assignment
// func (c *Constraint) Set(v []byte, count uint64, txn *badger.Txn) (err error) {
func (c *Constraint) Set(v I, count uint64) {
	place := (c.Place + 1) % 3
	prefix := types.TriplePrefixes[place]

	if _, is := c.M.(VariableNode); is {
		c.Prefix = types.AssembleKey(prefix, v, c.n, nil)
		c.m = v
	} else if _, is := c.N.(VariableNode); is {
		c.Prefix = types.AssembleKey(prefix, c.m, v, nil)
		c.n = v
	}

	c.Count = count
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
func (cs ConstraintSet) Seek(v I) I {
	var count int
	l := len(cs)
	for i := 0; count < l; i = (i + 1) % l {
		c := cs[i]
		next := c.Seek(v)
		if next == nil {
			return nil
		} else if bytes.Equal(next, v) {
			count++
		} else {
			count = 1
			v = next
		}
	}
	return v
}

// Next value (could be improved to not double-check cursor[0])
func (cs ConstraintSet) Next() (next I) {
	c := cs[0]
	c.Iterator.Next()
	if next = c.value(); next != nil {
		next = cs.Seek(next)
	}
	return
}

// A ConstraintMap is a map of string variable labels to constraint sets.
type ConstraintMap map[int]ConstraintSet

// Len returns the total number of constraints in the constraint map
func (cm ConstraintMap) Len() (l int) {
	for _, cs := range cm {
		l += len(cs)
	}
	return
}

// A ConstraintGraph associates ids with Variable maps.
type ConstraintGraph struct {
	Variables []*Variable
	Domain    []string
	Map       map[string]int
	Cache     []*V
	Pivot     int
	In        [][]int
	Out       [][]int
}

func (g *ConstraintGraph) String() string {
	s := "----- Constraint Graph -----\n"
	for i, id := range g.Domain {
		s += fmt.Sprintf("---- %s ----\n%s\n", id, g.Variables[i].String())
	}
	s += fmt.Sprintln("----- End of Constraint Graph -----")
	return s
}

// Close just calls Close on its child constraints
func (g *ConstraintGraph) Close() {
	if g != nil && g.Variables != nil {
		for _, u := range g.Variables {
			u.Close()
		}
	}
}

// Sort interface functions
func (g *ConstraintGraph) Len() int { return len(g.Domain) }
func (g *ConstraintGraph) Swap(a, b int) {
	g.Variables[a], g.Variables[b] = g.Variables[b], g.Variables[a]
	g.Domain[a], g.Domain[b] = g.Domain[b], g.Domain[a]
}

// TODO: put more thought into the sorting heuristic.
// Right now the variables are sorted their norm: in
// increasing order of their length-normalized sum of
// the squares of the counts of all their constraints (of any degree).
func (g *ConstraintGraph) Less(a, b int) bool {
	A, B := g.Variables[a], g.Variables[b]
	return (float32(A.Norm) / float32(A.Size)) < (float32(B.Norm) / float32(B.Size))
}

// Get retrieves an Variable or creates one if it doesn't exist.
func (g *ConstraintGraph) Get(id string) (v *Variable, i int) {
	if g.Map == nil {
		v = &Variable{}
		g.Map = map[string]int{id: i}
		g.Domain = []string{id}
		g.Variables = []*Variable{v}
	} else if index, has := g.Map[id]; has {
		v, i = g.Variables[index], index
	} else {
		v, i = &Variable{}, len(g.Domain)
		g.Map[id] = i
		g.Domain = append(g.Domain, id)
		g.Variables = append(g.Variables, v)
	}
	return
}

// // GetIndex is a convenience method for retrieving a variable by its integer index
// func (g *ConstraintGraph) GetIndex(i int) (string, *Variable) {
// 	p := g.Domain[i]
// 	return p, g.Variables[p]
// }

func (g *ConstraintGraph) insertDZ(u string, c *Constraint, txn *badger.Txn) (err error) {
	// For z-degree constraints we get the *count* with an index key
	// and set the *prefix* to a major key (although we could also use a minor key)

	variable, _ := g.Get(u)
	// if variable.DZ == nil {
	// 	variable.DZ = ConstraintSet{c}
	// } else {
	// 	variable.DZ = append(variable.DZ, c)
	// }
	if variable.CS == nil {
		variable.CS = ConstraintSet{c}
	} else {
		variable.CS = append(variable.CS, c)
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

	variable, _ := g.Get(u)
	// if variable.D1 == nil {
	// 	variable.D1 = ConstraintSet{c}
	// } else {
	// 	variable.D1 = append(variable.D1, c)
	// }
	if variable.CS == nil {
		variable.CS = ConstraintSet{c}
	} else {
		variable.CS = append(variable.CS, c)
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

	variable, _ := g.Get(u)
	if variable.D2 == nil {
		variable.D2 = ConstraintMap{}
	}

	_, j := g.Get(v)
	if cs, has := variable.D2[j]; has {
		variable.D2[j] = append(cs, c)
	} else {
		variable.D2[j] = ConstraintSet{c}
	}

	if variable.CS == nil {
		variable.CS = ConstraintSet{c}
	} else {
		variable.CS = append(variable.CS, c)
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
