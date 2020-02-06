package query

import (
	"bytes"
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	proto "github.com/gogo/protobuf/proto"
	types "github.com/underlay/styx/types"
)

// A constraint to an occurrence of a variable in a dataset
type constraint struct {
	index    int               // The index of the triple within the dataset
	place    types.Permutation // The term (subject = 0, predicate = 1, object = 2) within the triple
	m        Node              // The next ((Place + 1) % 3) element in the triple
	mID      I                 // a convience slot for the []byte of M, if it is known at the time
	n        Node              // The previous ((Place + 2) % 3) element in the triple
	nID      I                 // a convience slot for the []byte of N, if it is known at the time
	count    uint64            // The number of triples that satisfy the Constraint
	prefix   []byte            // This is m and n (whichever exist) appended to the appropriate prefix
	iterator *badger.Iterator
	dual     *constraint // If (M or N) is a blank node, this is a pointer to their dual struct
}

// cache is a struct for holding cached value states
type cache = struct {
	i int
	j int
	c uint64
}

func (c *constraint) save(i, j int) cache {
	return cache{i, j, c.count}
}

func (c *constraint) printM() (s string) {
	if b, is := c.m.(VariableNode); is {
		s = string(b)
		if c.mID != nil {
			s += fmt.Sprintf(" = %02d", binary.BigEndian.Uint64(c.mID))
		}
	} else if i, is := c.m.(*types.Index); is {
		s = i.String()
	}
	return
}

func (c *constraint) printN() (s string) {
	if b, is := c.n.(VariableNode); is {
		s = string(b)
		if c.nID != nil {
			s += fmt.Sprintf(" = %02d", binary.BigEndian.Uint64(c.nID))
		}
	} else if i, is := c.n.(*types.Index); is {
		s = i.String()
	}
	return
}

// Sources can only be called on a first-degree constraint
// and it returns the unmarshalled SourceList from the value
// of the badger iterator's current item
func (c *constraint) Sources(value I, txn *badger.Txn) (statements []*types.Statement, err error) {
	var item *badger.Item
	if c.place == 0 {
		item = c.iterator.Item()
	} else {
		var s, p, o I
		if c.place == 1 {
			s, p, o = c.nID, value, c.mID
		} else if c.place == 2 {
			s, p, o = c.mID, c.nID, value
		}
		key := types.AssembleKey(types.TriplePrefixes[0], s, p, o)
		if item, err = txn.Get(key); err != nil {
			return
		}
	}

	sources := &types.SourceList{}
	err = item.Value(func(val []byte) error { return proto.Unmarshal(val, sources) })
	if err != nil {
		return
	}

	statements = sources.GetSources()
	return
}

func (c *constraint) String() string {
	if len(c.prefix) == 9 {
		return fmt.Sprintf(
			"(p%d {%s | %s} %s:%02d#%d)",
			c.place,
			c.printM(), c.printN(),
			string(c.prefix[0]),
			binary.BigEndian.Uint64(c.prefix[1:]),
			c.count,
		)
	} else if len(c.prefix) == 17 {
		return fmt.Sprintf(
			"(p%d {%s | %s} %s:%02d:%02d#%d)",
			c.place,
			c.printM(), c.printN(),
			string(c.prefix[0]),
			binary.BigEndian.Uint64(c.prefix[1:9]),
			binary.BigEndian.Uint64(c.prefix[9:]),
			c.count,
		)
	} else {
		return "<<<invalid constraint>>>"
	}
}

// Close the constraint's cursor's iterator, if it exists
func (c *constraint) Close() {
	if c.iterator != nil {
		c.iterator.Close()
		c.iterator = nil
	}
}

func (c *constraint) value() (v I) {
	if c.iterator.ValidForPrefix(c.prefix) {
		item := c.iterator.Item()
		key := item.KeyCopy(make([]byte, len(c.prefix)+8))
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
func (c *constraint) Next() I {
	c.iterator.Next()
	return c.value()
}

// Seek advances the cursor to the first value equal to
// or greater than given byte slice.
func (c *constraint) Seek(v I) I {
	key := make([]byte, len(c.prefix)+8)
	copy(key[:len(c.prefix)], c.prefix)
	if v != nil {
		copy(key[len(c.prefix):], v)
	}
	c.iterator.Seek(key)
	return c.value()
}

// Set the value of a temporary assignment
// func (c *Constraint) Set(v []byte, count uint64, txn *badger.Txn) (err error) {
func (c *constraint) Set(v I, count uint64) {
	place := (c.place + 1) % 3
	prefix := types.TriplePrefixes[place]

	if _, is := c.m.(VariableNode); is {
		c.prefix = types.AssembleKey(prefix, v, c.nID, nil)
		c.mID = v
	} else if _, is := c.n.(VariableNode); is {
		c.prefix = types.AssembleKey(prefix, c.mID, v, nil)
		c.nID = v
	}

	c.count = count
}

func (c *constraint) getCount(txn *badger.Txn) (count uint64, err error) {
	// ONLY call getCount on first-degree constraints _or_ second-degree constraints
	// whose other variable has been temporarily set!!
	if c.nID == nil || c.mID == nil {
		return
	}

	// Assemble a major key using the two constant values m and n
	key := types.AssembleKey(types.MajorPrefixes[(c.place+1)%3], c.mID, c.nID, nil)
	// **We could equally as easily have assembled a minor key to get the count**
	// key = types.AssembleKey(types.MinorPrefixes[(c.Place+2)%3], c.n, c.m, nil)

	var item *badger.Item
	if item, err = txn.Get(key); err == badger.ErrKeyNotFound {
		return 0, nil
	} else if err != nil {
		return
	}

	err = item.Value(func(val []byte) error {
		count = binary.BigEndian.Uint64(val)
		return nil
	})

	return
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
func (cs constraintSet) Seek(v I) I {
	var count int
	l := cs.Len()
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
func (cs constraintSet) Next() (next I) {
	c := cs[0]
	c.iterator.Next()
	if next = c.value(); next != nil {
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
