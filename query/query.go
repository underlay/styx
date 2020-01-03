package query

import (
	"encoding/binary"
)

// i, j, k, l... are int indices
// p, q, r... are string variable labels
// u, v, w... are *Variable pointers
// x, y... are dependency slice indices, where e.g. g.In[p][x] == i

func (g *cursorGraph) initial() (err error) {
	l := g.Len()

	var ok bool

	for i, u := range g.variables {
		if u.value == nil {
			for u.value = u.Seek(u.root); u.value == nil; u.value = u.Seek(u.root) {
				if ok, err = g.tick(i, 0, g.cache); err != nil {
					return
				} else if !ok {
					return
				}
			}
		}

		// We've got a non-nil value for u!
		g.pushTo(u, i, l)
		for j, saved := range g.cache[:i] {
			if saved != nil {
				g.cache[j] = nil
				if i+1 < l {
					g.pushTo(g.variables[j], i, l)
				}
			}
		}
	}

	return
}

func (g *cursorGraph) next(i int) (tail int, err error) {
	var ok bool
	tail = g.Len()
	// Okay so we start at the index given to us
	for i >= 0 {
		u := g.variables[i]
		self := u.value
		// Try naively getting another value from u
		u.value = u.Next()
		if u.value == nil {
			// It didn't work :-/
			// This means we reset u, decrement i, and continue
			u.value = u.Seek(self)
			i--
			continue
		}

		// It worked! We have a new value for u.
		// New we need to propagate it to the rest of the
		// variables in g.domain[i+1:], if they exist,
		// and make sure they're satisfied.

		// To do that, first push u's value to the rest of the domain
		err = g.pushTo(u, i, g.Len())
		if err != nil {
			return
		}

		// Now set the `cursor` variable to our current index.
		// If we fail to satisfy a variable in d.gomain[i:1:] while
		// propagating here, then we'll set cursor to that failure index.
		cursor := i
		// Don't recurse on i!
		g.blacklist[i] = true
		for _, j := range g.out[i] {
			v := g.variables[j]
			// We have to give g.tick(j, ...) a fresh cache here.
			// TODO: there some memory saving stuff to be done about caches :-/
			d := make([]*V, j)

			// Okay - what we want is a new value for v. Since we pushed a new u.value
			// into v, we have to "start all over" with v.Seek(v.root).
			// This might mean that v.Seek(v.root) gives us a non-nil value - that's great!
			// Then we don't even enter the loop. But if v.Seek(v.root) _doesn't_ give us a
			// value, then we have to use g.tick() on j to tick j's dependencies into their
			// next valid state (passing the fresh cache in). That will either irrecovably
			// fail or give us a new state to try v.Seek(v.root) again on.
			for v.value = v.Seek(v.root); v.value == nil; v.value = v.Seek(v.root) {
				ok, err = g.tick(j, i, d)
				if err != nil {
					return
				} else if !ok {
					break
				}
			}

			// Cool! Either v.value == nil and there are no more solutions to the query...
			if v.value == nil {
				cursor = j
				break
			}

			// ... or v.value != nil and we can push it to the rest of the domain!
			err = g.pushTo(v, j, g.Len())
			if err != nil {
				return
			}

			// One really trick bit is that when we used g.tick(j, i, d) to satisfy v,
			// it might have changed some other previous values in the domain.
			// Specifically, if might have changed:
			// - variables between i and j that are in out[i]
			// - variables between i and j that are NOT in out[i] (!!)
			// It will NOT change any variables before i (or i itself)
			// So we can't use out[i] here - we have to use the cache d that we passed
			// into g.tick(j, i, d) to tell which variables were changed.
			for x, saved := range d[i+1:] {
				if saved != nil {
					// This means that w has a new value that needs to be pushed to the
					// rest of the domain.
					k := i + 1 + x
					w := g.variables[k]
					err = g.pushTo(w, k, g.Len())
					if err != nil {
						return
					}

					// If g.cache[k] hasn't been saved yet, save it here.
					if g.cache[k] == nil {
						g.cache[k] = saved
					}
				}
			}
		}

		g.blacklist[i] = false

		// Cool - either we completed the loop over g.out[i] naturally,
		// or we broke out early and set cursor = j. Check for that here:
		if cursor == i {
			// Success!! We brought all of the variables before and after i into
			// a valid state. Clear the cache and return.
			clear(g.cache)
			tail = i
			return
		}

		// Oh well - we broke out early at cursor = j, because the v.Seek / g.tick(j...)
		// loop didn't give us a result. Now we restore the values that changed...
		err = g.restore(g.cache[:cursor+1], g.Len())
		if err != nil {
			return
		}

		// ... and clear the cache, but don't decrement i.
		// We want to try the same variable i over and over until it gives up!
		// (I'm not actually sure if we need to clear the cache here...)
		clear(g.cache)
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
func (g *cursorGraph) tick(i, min int, delta []*V) (ok bool, err error) {
	next := make([]*V, i)

	// The biggest outer loop is walking backwards over g.In[i]
	x := len(g.in[i])
	for x > 0 {
		j := g.in[i][x-1]

		if j <= min {
			return false, g.restore(next, i)
		} else if g.blacklist[j] {
			x--
			continue
		}

		v := g.variables[j]

		self := v.save()

		if v.value = v.Next(); v.value == nil {
			// That sucks. Now we need to restore the value
			// that was changed and decrement x.
			v.value = self.I
			v.Seek(v.value)
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
			g.blacklist[j] = true
			for _, k := range g.out[j] {
				if k >= i {
					break
				}

				w := g.variables[k]

				if next[k] == nil {
					next[k] = w.save()
				}

				d := make([]*V, k)

				// Here we keep seeking and ticking until we have a real value.
				for w.value = w.Seek(w.root); w.value == nil; w.value = w.Seek(w.root) {
					if ok, err = g.tick(k, min, d); err != nil {
						return
					} else if ok {
						continue
					} else if err = g.restore(d, i); err != nil {
						return
					} else {
						break
					}
				}

				if w.value == nil {
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
						if err = g.pushTo(g.variables[l], l, i); err != nil {
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
			g.blacklist[j] = false

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
						if err = g.pushTo(g.variables[l], i, g.Len()); err != nil {
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

func (g *cursorGraph) restore(cache []*V, max int) (err error) {
	for i, saved := range cache {
		// If the variable at k has been modified by the
		// (potentially) multiple recursive calls to tick,
		// then reset it to its previous state.
		if saved != nil {
			u := g.variables[i]
			u.load(saved)
			// Push the restored state through the max
			if err = g.pushTo(u, i, max); err != nil {
				return
			}
		}
	}
	return
}

func (g *cursorGraph) pushTo(u *variable, min, max int) (err error) {
	for j, cs := range u.d2 {
		if j >= min && j < max {
			w := g.variables[j]

			// Update the incoming D2 constraints by using .Dual to find them
			for _, c := range cs {
				// Since v has a value, all of its constraints are in consensus.
				// That means we can freely access their iterators!
				// In this case, all the iterators for the outgoing v.D2s have
				// values that are the counts (uint64) of them *and their dual*.
				// This is insanely elegant...
				item := c.iterator.Item()
				var count uint64
				err = item.Value(func(val []byte) error {
					count = binary.BigEndian.Uint64(val)
					return nil
				})

				if err != nil {
					return
				}

				c.dual.Set(u.value, count)
			}

			// Set the value to nil, like I promised you earlier.
			w.value = nil
			w.Sort()
		}
	}
	return
}
