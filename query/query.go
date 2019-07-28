package query

import (
	badger "github.com/dgraph-io/badger"
)

// Solve solves the graph
func (g *ConstraintGraph) Solve(txn *badger.Txn) (err error) {
	for i := 0; i < len(g.Slice); i++ {
		if err = g.Tick(i, txn); err != nil {
			return
		} else if _, u := g.GetIndex(i); u.Value == nil {
			return ErrEmptyIntersect
		}
	}
	return
}

// Tick seeks to the first *cumulatively satisfying value* for the given index.
func (g *ConstraintGraph) Tick(i int, txn *badger.Txn) (err error) {
	// i, j, k, l... are *int* indices
	// p, q, r... are *string* variable labels
	// u, v, w... are *Variable instances
	// x, y... are *dependency slice indices*, where e.g. g.In[p][x] == i
	p, u := g.GetIndex(i)
	u.Value = u.Seek(u.Root)
	if u.Value != nil {
		// We got a valid value for u!
		err = g.propagate(u.D2, i, len(g.Slice), u.Value, txn)
		return
	}

	in := g.In[p]
	out := g.Out[p]
	length := len(in)

	// First we need to save a snapshot of the current values, counts,
	// and prefixes of all the variables so that we can reset them
	// when we backtrack. This just iterates over all the dependencies
	// of i and puts their values into a map.

	values := make(map[string][]byte, length)
	counts := map[[2]string][]uint64{}
	prefixes := map[[2]string][][]byte{}

	for _, j := range append(in, out...) {
		q, v := g.GetIndex(j)
		values[q] = v.Value[:]

		// Now iterate over the second-degree constraints *in-between* j and i
		for r, cs := range v.D2 {
			if g.Map[r] > j {
				continue
			}
			key := [2]string{q, r}
			counts[key] = make([]uint64, len(cs))
			prefixes[key] = make([][]byte, len(cs))
			for k, c := range cs {
				counts[key][k] = c.Count
				prefixes[key][k] = c.Prefix
			}
		}
	}

	// Now that we've saved the current state, we can start
	var x = length - 1
	for x >= 0 {
		j := in[x]
		q, v := g.GetIndex(j)
		if v.Value = v.Next(); v.Value == nil {
			// Reset everything, *including* the current index
			for y := x; y < length; y++ {
				g.reset(in[y], values, counts, prefixes)
			}
			x--
			continue
		}

		if err = g.propagate(v.D2, j, i, v.Value, txn); err != nil {
			return
		}

		// Now that the updates have been propagated we need to iterate through the
		// dependencies in between j and i, getting the next value from _them_,
		// and propagating _that_!
		for _, k := range g.Out[q] {
			_, w := g.GetIndex(k)
			if w.Value = w.Seek(w.Root); w.Value == nil || k == i {
				break
			}

			if err = g.propagate(w.D2, k, len(g.Slice), w.Value, txn); err != nil {
				return
			}
		}

		if u.Value == nil {
			// Reset everything, *excluding* the current index
			for y := x + 1; y < length; y++ {
				g.reset(in[y], values, counts, prefixes)
			}
			continue
		}

		// We got a valid new value for u!
		return
	}

	// We ran out of variables to backtrack on!
	// This means the variable is unsatisfiable :-/
	return
}

func (g *ConstraintGraph) propagate(
	cs ConstraintMap,
	j int, i int,
	value []byte,
	txn *badger.Txn,
) (err error) {
	// We have the next value for the dependency, so now we set temporary
	// values and update counts for the duals of all the second-degree
	// constraints that "point forward". This effectively upgrades them
	// to function as first-degree dependencies.
	for r, cs := range cs {
		w := g.Index[r]
		if g.Map[r] < j || g.Map[r] > i {
			continue
		}

		for _, c := range cs {
			if err = c.Dual.Set(value, txn); err != nil {
				return
			}
		}

		// Now that we'e changed some of the counts, we need to re-sort
		w.Sort()
	}
	return
}

func (g *ConstraintGraph) reset(
	i int,
	values map[string][]byte,
	counts map[[2]string][]uint64,
	prefixes map[[2]string][][]byte,
) {
	p, u := g.GetIndex(i)
	u.Value = values[p]

	for q, cs := range u.D2 {
		if g.Map[q] > i {
			continue
		}
		key := [2]string{p, q}
		for k, c := range cs {
			c.Count = counts[key][k]
			c.Prefix = prefixes[key][k]
			if u.Value != nil {
				c.Seek(u.Value)
			} else {
				c.Seek(u.Root)
			}
		}
	}
}
