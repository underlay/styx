package query

import (
	"encoding/binary"

	badger "github.com/dgraph-io/badger"
	types "github.com/underlay/styx/types"
)

// i, j, k, l... are int indices
// p, q, r... are string variable labels
// u, v, w... are *Variable pointers
// x, y... are dependency slice indices, where e.g. g.In[p][x] == i

// Prov!
type Prov = []map[int][]*types.Statement

// Next advances to the next solution in the domain and returns a *tail slice* of assignment ids
func (g *ConstraintGraph) Next(txn *badger.Txn) (tail []I, prov Prov, err error) {
	l := g.Len()
	blacklist := make([]bool, l)

	var ok bool
	if g.Cache == nil {
		g.Cache = make([]*V, l)
		for i, u := range g.Variables {
			if u.Value == nil {
				for u.Value = u.Seek(u.Root); u.Value == nil; u.Value = u.Seek(u.Root) {
					if ok, err = g.tick(i, 0, blacklist, g.Cache); err != nil {
						return
					} else if !ok {
						return
					}
				}
			}

			// We've got a non-nil value for u!
			g.pushTo(u, i, l)
			for j, saved := range g.Cache[:i] {
				if saved != nil {
					g.pushTo(g.Variables[j], i, l)
					g.Cache[j] = nil
				}
			}
		}

		tail = make([]I, g.Len())
		for i, u := range g.Variables {
			tail[i] = u.Value
		}
		return
	}

	i := g.Pivot - 1
	for i >= 0 {
		u := g.Variables[i]
		self := u.Value
		if u.Value = u.Next(); u.Value == nil {
			u.Value = u.Seek(self)
			i--
			continue
		}

		if err = g.pushTo(u, i, g.Len()); err != nil {
			return
		}

		cursor := i
		blacklist[i] = true
		for _, j := range g.Out[i] {
			v := g.Variables[j]
			d := make([]*V, j)
			for v.Value = v.Seek(v.Root); v.Value == nil; v.Value = v.Seek(v.Root) {
				if ok, err = g.tick(j, i, blacklist[:j], d); err != nil {
					return
				} else if !ok {
					break
				}
			}

			if v.Value == nil {
				cursor = j
				break
			} else if err = g.pushTo(v, j, g.Len()); err != nil {
				return
			}

			for x, saved := range d[i:] {
				if saved != nil {
					k := i + x
					w := g.Variables[k]
					if err = g.pushTo(w, k, g.Len()); err != nil {
						return
					}
					if g.Cache[k] == nil {
						g.Cache[k] = saved
					}
				}
			}
		}

		blacklist[i] = false

		if cursor == i {
			// success!!
			tail = make([]I, g.Len()-i)
			for x, v := range g.Variables[i:] {
				tail[x] = v.Value
			}
			clear(g.Cache)
			// prov = make([]map[int][]*types.Statement, len(tail))
			return
		}

		if err = g.restore(g.Cache[:cursor+1], g.Len()); err != nil {
			return
		}
		clear(g.Cache)
	}
	return
}

func clear(delta []*V) {
	for i, saved := range delta {
		if saved != nil {
			delta[i] = nil
		}
	}
}

// tick advances the given index's dependencies into their next valid state, giving
// the variable at the index (at least) one new value at its incoming constraints.
// tick makes two promises. The first is that it will leave your blacklist in the same
// state that it found it. The second is that either a) it will return ok = false, the
// variables will be in their initial states, and delta is in its initial state; or b)
// it will return ok = true, the variables rest in a new consensus state, every changed
// variable's initial state is added to delta if it doesn't already exist, and no
// non-nil element of delta is overwritten.
func (g *ConstraintGraph) tick(i, min int, blacklist []bool, delta []*V) (ok bool, err error) {
	next := make([]*V, i)

	// The biggest outer loop is walking backwards over g.In[i]
	x := len(g.In[i])
	for x > 0 {
		j := g.In[i][x-1]

		if j < min {
			return false, g.restore(next, i)
		} else if blacklist[j] {
			x--
			continue
		}

		v := g.Variables[j]

		self := v.save()

		if v.Value = v.Next(); v.Value == nil {
			// That sucks. Now we need to restore the value
			// that was changed and decrement x.
			v.Value = self.I
			v.Seek(v.Value)
			x--
		} else {
			// We got a non-nil value for v, so now we
			// propagate between j and i, then crawl forward
			// over the indices in g.Out[q] that are less than i
			// and seek to their new values.

			// Propagate up to but not including i
			if err = g.pushTo(v, j, i); err != nil {
				return
			}

			// Fantastic. Now that we've propagated the value we found for v,
			// we start "the crawl" from j to i, seeking to the new satisfying root
			// and recursing on tick when necessary.
			cursor := j
			blacklist[j] = true
			for _, k := range g.Out[j] {
				if k >= i {
					break
				}

				w := g.Variables[k]

				if next[k] == nil {
					next[k] = w.save()
				}

				d := make([]*V, k)

				// Here we keep seeking and ticking until we have a real value.
				for w.Value = w.Seek(w.Root); w.Value == nil; w.Value = w.Seek(w.Root) {
					// There's no real reason to pass blacklist[:k] instead of just blacklist
					// here, but it feels cleaner since we know that it's all it needs.
					if ok, err = g.tick(k, min, blacklist[:k], d); err != nil {
						return
					} else if ok {
						continue
					} else if err = g.restore(d, i); err != nil {
						return
					} else {
						break
					}
				}

				if w.Value == nil {
					// We were unable to complete the crawl.
					// We've already reset our state.
					// This is how far we got:
					cursor = k + 1
					break
				}

				// We got a real value for w! Now we propagate the affected values
				// through i and stash them into next if they're not there already,
				// and then continue with the tick-crawl.
				if err = g.pushTo(w, k, i); err != nil {
					return
				}
				for l, saved := range d {
					if saved != nil {
						if err = g.pushTo(g.Variables[l], l, i); err != nil {
							return
						}
						if next[l] == nil {
							next[l] = saved
						}
					}
				}
			}

			// We need to *unset* the blacklist after recursing.
			// Variables are only blacklisted when they appear as
			// a parent in the call stack - they might be visited
			// twice as siblings in the call tree, etc.
			blacklist[j] = false

			if cursor == j {
				// Hooray!
				// Now here we need to push every affected value
				// through to the rest of the domain
				delta[j] = self
				for l, saved := range next {
					if saved != nil {
						if delta[l] == nil {
							delta[l] = saved
						}
						if err = g.pushTo(g.Variables[l], i, g.Len()); err != nil {
							return
						}
					}
				}
				return true, nil
			}

			// This means we reset (all) those affected to their previous state
			if err = g.restore(next, i); err != nil {
				return
			}
		}
	}
	return
}

func (g *ConstraintGraph) restore(cache []*V, max int) (err error) {
	for i, saved := range cache {
		// If the variable at k has been modified by the
		// (potentially) multiple recursive calls to tick,
		// then reset it to its previous state.
		if saved != nil {
			u := g.Variables[i]
			u.load(saved)
			// Push the restored state through the max
			if err = g.pushTo(u, i, max); err != nil {
				return
			}
		}
	}
	return
}

func (g *ConstraintGraph) pushTo(u *Variable, min, max int) (err error) {
	for j, cs := range u.D2 {
		if j >= min && j < max {
			w := g.Variables[j]

			// Update the incoming D2 constraints by using .Dual to find them
			for _, c := range cs {
				// Since v has a value, all of its constraints are in consensus.
				// That means we can freely access their iterators!
				// In this case, all the iterators for the outgoing v.D2s have
				// values that are the counts (uint64) of them *and their dual*.
				// This is insanely elegant...
				item := c.Iterator.Item()
				val := make([]byte, 8)
				if val, err = item.ValueCopy(val); err != nil {
					return
				}
				count := binary.BigEndian.Uint64(val)
				c.Dual.Set(u.Value, count)
			}

			// Set the value to nil, like I promised you earlier.
			w.Value = nil
			w.Sort()
		}
	}
	return
}
