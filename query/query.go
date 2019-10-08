package query

import (
	badger "github.com/dgraph-io/badger"
	types "github.com/underlay/styx/types"
)

// i, j, k, l... are int indices
// p, q, r... are string variable labels
// u, v, w... are *Variable pointers
// x, y... are dependency slice indices, where e.g. g.In[p][x] == i

// Solve the constraint graph, one variable at as time
func (g *ConstraintGraph) Solve(txn *badger.Txn) (err error) {
	_, u := g.GetIndex(0)
	u.Value = u.Root
	for i := 0; i < len(g.Slice); i++ {
		if err = g.solve(i, txn); err != nil {
			return
		} else if _, u := g.GetIndex(i); u.Value == nil {
			return ErrEmptyJoin
		}
	}

	g.Root = make(map[string][]byte, len(g.Slice))
	for p, u := range g.Index {
		g.Root[p] = u.Value
	}

	return
}

// solve for the first *cumulatively satisfying value* at the given index.
func (g *ConstraintGraph) solve(i int, txn *badger.Txn) (err error) {
	p, u := g.GetIndex(i)

	if u.Value == nil {
		u.Value = u.Seek(u.Root)
	}

	if u.Value != nil {
		// We got a valid value for u!
		return g.propagate(u.D2, i, len(g.Slice), u.Value, txn)
	}

	in, out := g.In[p], g.Out[p]

	// First we need to save a snapshot of the current values, counts,
	// and prefixes of all the variables so that we can reset them
	// when we backtrack. This just iterates over all the dependencies
	// of i and puts their values into a map.

	values := make([][]byte, len(in))
	counts := map[[2]string][]uint64{}
	prefixes := map[[2]string][][]byte{}

	for x, j := range in {
		q, v := g.GetIndex(j)
		values[x] = v.Value

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
	var x = len(in) - 1
	for x >= 0 {
		j := in[x]
		q, v := g.GetIndex(j)
		if v.Value = v.Next(); v.Value == nil {
			// Reset everything, *including* the current index
			for y, k := range in[x:] {
				g.reset(k, values[x+y], counts, prefixes)
			}
			x--
			continue
		}

		if err = g.propagate(v.D2, j, i+1, v.Value, txn); err != nil {
			return
		}

		// Now that the updates have been propagated we need to iterate through the
		// dependencies in between j and i, getting the next value from _them_,
		// and propagating _that_!
		for _, k := range g.Out[q] {
			_, w := g.GetIndex(k)
			if w.Value = w.Seek(w.Root); w.Value == nil {
				break
			}

			if err = g.propagate(w.D2, k, len(g.Slice), w.Value, txn); err != nil {
				return
			}

			if k == j {
				break
			}
		}

		if u.Value != nil {
			// We got a valid new value for u!
			for _, k := range out {
				if err = g.propagate(u.D2, k, len(g.Slice), u.Value, txn); err != nil {
					return
				}
			}

			return
		}

		// Reset everything, *excluding* the current index
		for y, k := range in[x+1:] {
			g.reset(k, values[x+1+y], counts, prefixes)
		}
		x = len(in) - 1
	}

	// We ran out of variables to backtrack on!
	// This means the variable is unsatisfiable :-/
	return
}

func (g *ConstraintGraph) GetSources(txn *badger.Txn) (sources map[int]*types.SourceList, err error) {
	sources = map[int]*types.SourceList{}
	for q, v := range g.Index {
		// Collect the sources for every first-degree constriant
		for _, c := range v.D1 {
			if sources[c.Index], err = c.Sources(v.Value, txn); err != nil {
				return
			}
		}

		// Collect the sources for every second-degree constriant
		for r, cs := range v.D2 {
			if g.Map[r] < g.Map[q] {
				for _, c := range cs {
					if sources[c.Index], err = c.Sources(v.Value, txn); err != nil {
						return
					}
				}
			}
		}
	}
	return
}

// Collect must be called *after* Solve.
func (g *ConstraintGraph) Collect(n int, sources []map[int]*types.SourceList, txn *badger.Txn) (results [][][]byte, err error) {
	if n < 1 {
		return
	}

	results = make([][][]byte, 1, n)
	results[0] = make([][]byte, len(g.Slice))
	for i, p := range g.Slice {
		results[0][i] = g.Index[p].Value
	}

	if sources[0], err = g.GetSources(txn); err != nil {
		return
	}

	values := make([][]byte, g.Pivot)

	var x = 1
	var i = g.Pivot - 1
	for i >= 0 && x < n {
		counts := map[[2]string][]uint64{}
		prefixes := map[[2]string][][]byte{}

		for y, q := range g.Slice[i:g.Pivot] {
			j := i + y
			v := g.Index[q]

			values[j] = v.Value

			// Now iterate over the second-degree constraints
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

		p, u := g.GetIndex(i)
		if u.Value = u.Next(); u.Value == nil {
			// Reset everything, *including* the current index
			for j := i; j < g.Pivot; j++ {
				g.reset(j, values[j], counts, prefixes)
			}
			i--
			continue
		} else if err = g.propagate(u.D2, i, len(g.Slice), u.Value, txn); err != nil {
			return
		}

		// Now that the updates have been propagated we need to iterate through the
		// dependencies in between j and i, getting the next value from _them_,
		// and propagating _that_!
		failed := false
		for _, j := range g.Out[p] {
			_, v := g.GetIndex(j)
			if v.Value = v.Seek(v.Root); v.Value == nil {
				failed = true
				break
			} else if err = g.propagate(v.D2, j, len(g.Slice), v.Value, txn); err != nil {
				return
			}
		}

		if failed {
			// Reset everything, *excluding* the current index
			for _, j := range g.Out[p] {
				g.reset(j, values[j], counts, prefixes)
			}
		} else {
			// We got a valid new value!
			results = append(results, make([][]byte, len(g.Slice)))
			for j, q := range g.Slice {
				results[x][j] = g.Index[q].Value
			}

			if sources[x], err = g.GetSources(txn); err != nil {
				return
			}

			i = g.Pivot - 1
			x++
		}
	}
	return
}

func (g *ConstraintGraph) propagate(
	cs ConstraintMap,
	i int, max int,
	value []byte,
	txn *badger.Txn,
) (err error) {
	// We have the next value for the dependency, so now we set temporary
	// values and update counts for the duals of all the second-degree
	// constraints that "point forward". This effectively upgrades them
	// to function as first-degree dependencies.
	for q, cs := range cs {
		j, v := g.Map[q], g.Index[q]
		if j < i || j >= max {
			continue
		}

		// Set the value to nil, like I promised you earlier.
		v.Value = nil
		for _, c := range cs {
			if err = c.Dual.Set(value, txn); err != nil {
				return
			}
		}

		// Now that we'e changed some of the counts, we need to re-sort
		v.Sort()
	}
	return
}

func (g *ConstraintGraph) reset(
	i int,
	value []byte,
	counts map[[2]string][]uint64,
	prefixes map[[2]string][][]byte,
) {
	p, u := g.GetIndex(i)
	u.Value = value

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
		// delete(counts, key)
		// delete(prefixes, key)
	}
}
