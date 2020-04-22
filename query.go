package styx

import (
	"encoding/binary"
)

// i, j, k, l... are int indices
// p, q, r... are string variable labels
// u, v, w... are *Variable pointers
// x, y... are dependency slice indices, where e.g. iter.In[p][x] == i

func (iter *Iterator) next(i int) (tail int, err error) {
	var ok bool
	tail = iter.Len()
	// Okay so we start at the index given to us
	for i >= 0 {
		u := iter.variables[i]
		// Try naively getting another value from u
		u.value = u.Next()
		if u.value == NIL {
			// It didn't work :-/
			// This means we reset u, decrement i, and continue
			u.value = u.Seek(u.root)
			err = iter.push(u, i, iter.Len())
			if err != nil {
				return
			}

			i--
			continue
		}

		// It worked! We have a new value for u.
		// New we need to propagate it to the rest of the
		// variables in iter.domain[i+1:], if they exist,
		// and make sure they're satisfied.

		// To do that, first push u's value to the rest of the domain
		err = iter.push(u, i, iter.Len())
		if err != nil {
			return
		}

		// Now set the `cursor` variable to our current index.
		// If we fail to satisfy a variable in iter.out[i] while
		// propagating here, then we'll set cursor to that failure index.
		cursor := i
		// Don't recurse on i!
		iter.blacklist[i] = true
		for _, j := range iter.out[i] {
			v := iter.variables[j]
			// We have to give iter.tick(j, ...) a fresh cache here.
			// TODO: there some memory saving stuff to be done about caches :-/
			d := make([]*vcache, j)

			// Okay - what we want is a new value for v. Since we pushed a new u.value
			// into v, we have to "start all over" with v.Seek(v.root).
			// This might mean that v.Seek(v.root) gives us a non-nil value - that's great!
			// Then we don't even enter the loop. But if v.Seek(v.root) _doesn't_ give us a
			// value, then we have to use iter.tick() on j to tick j's dependencies into their
			// next valid state (passing the fresh cache in). That will either irrecovably
			// fail or give us a new state to try v.Seek(v.root) again on.
			for v.value = v.Seek(v.root); v.value == NIL; v.value = v.Seek(v.root) {
				ok, err = iter.tick(j, i, d)
				if err != nil {
					return
				} else if !ok {
					break
				}
			}

			// Cool! Either v.value == nil and there are no more solutions to the query...
			if v.value == NIL {
				cursor = j
				break
			}

			// ... or v.value != nil and we can push it to the rest of the domain!
			err = iter.push(v, j, iter.Len())
			if err != nil {
				return
			}

			// One really trick bit is that when we used iter.tick(j, i, d) to satisfy v,
			// it might have changed some other previous values in the domain.
			// Specifically, if might have changed:
			// - variables between i and j that are in out[i]
			// - variables between i and j that are NOT in out[i] (!!)
			// It will NOT change any variables before i (or i itself)
			// So we can't use out[i] here - we have to use the cache d that we passed
			// into iter.tick(j, i, d) to tell which variables were changed.
			for x, saved := range d[i+1:] {
				if saved != nil {
					k := i + 1 + x

					// If iter.cache[k] hasn't been saved yet, save it here.
					if iter.cache[k] == nil {
						iter.cache[k] = saved
					}
				}
			}
		}

		iter.blacklist[i] = false

		// Cool - either we completed the loop over iter.out[i] naturally,
		// or we broke out early and set cursor = j. Check for that here:
		if cursor == i {
			// Success!! We brought all of the variables before and after i into
			// a valid state. Clear the cache and return.
			clear(iter.cache)
			tail = i
			return
		}

		// Oh well - we broke out early at cursor = j, because the v.Seek / iter.tick(j...)
		// loop didn't give us a result. Now we restore the values that changed...
		err = iter.restore(iter.cache[:cursor+1], iter.Len())
		if err != nil {
			return
		}

		// ... and clear the cache, but don't decrement i.
		// We want to try the same variable i over and over until it gives up!
		// (I'm not actually sure if we need to clear the cache here...)
		clear(iter.cache)
	}
	return
}

func clear(delta []*vcache) {
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
func (iter *Iterator) tick(i, min int, delta []*vcache) (ok bool, err error) {
	next := make([]*vcache, i)

	// The biggest outer loop is walking backwards over iter.In[i]
	x := len(iter.in[i])
	for x > 0 {
		j := iter.in[i][x-1]

		if j <= min {
			return false, iter.restore(next, i)
		} else if iter.blacklist[j] {
			x--
			continue
		}

		v := iter.variables[j]

		self := v.save()

		if v.value = v.Next(); v.value == NIL {
			// That sucks. Now we need to restore the value
			// that was changed and decrement x.
			v.value = v.Seek(self.ID)
			x--
		} else {
			// We got a non-nil value for v, so now we
			// propagate between j and i, then crawl forward
			// over the indices in iter.Out[q] that are less than i
			// and seek to their new values.

			// Propagate up to but not including i
			if err = iter.push(v, j, i); err != nil {
				return
			}

			// Fantastic. Now that we've propagated the value we found for v,
			// we start "the crawl" from j to i, seeking to the new satisfying root
			// and recursing on tick when necessary.
			cursor := j
			iter.blacklist[j] = true
			for _, k := range iter.out[j] {
				if k >= i {
					break
				}

				w := iter.variables[k]

				if next[k] == nil {
					next[k] = w.save()
				}

				d := make([]*vcache, k)

				// Here we keep seeking and ticking until we have a real value.
				for w.value = w.Seek(w.root); w.value == NIL; w.value = w.Seek(w.root) {
					if ok, err = iter.tick(k, min, d); err != nil {
						return
					} else if ok {
						continue
					} else if err = iter.restore(d, i); err != nil {
						return
					} else {
						break
					}
				}

				if w.value == NIL {
					// We were unable to complete the crawl.
					// We've already reset our state.
					// This is how far we got:
					cursor = k + 1
					break
				}

				// We got a real value for w! Now we propagate the affected values
				// through i and stash them into next if they're not there already,
				// and then continue with the tick-crawl.
				err = iter.push(w, k, i)
				if err != nil {
					return
				}
				for l, saved := range d {
					if saved != nil {
						err = iter.push(iter.variables[l], l, i)
						if err != nil {
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
			iter.blacklist[j] = false

			if cursor == j {
				// Hooray!
				// Now here we need to push every affected value
				// through to the rest of the domain
				// delta[j] = self
				next[j] = self
				for l, saved := range next {
					if saved != nil {
						if delta[l] == nil {
							delta[l] = saved
						}
						err = iter.push(iter.variables[l], i, iter.Len())
						if err != nil {
							return
						}
					}
				}
				return true, nil
			}

			// This means we reset (all) those affected to their previous state
			err = iter.restore(next, i)
			if err != nil {
				return
			}
		}
	}
	return
}

func (iter *Iterator) restore(cache []*vcache, max int) (err error) {
	for i, saved := range cache {
		// If the variable at k has been modified by the
		// (potentially) multiple recursive calls to tick,
		// then reset it to its previous state.
		if saved != nil {
			u := iter.variables[i]
			iter.load(u, saved)
			// u.load(saved)
			// Push the restored state through the max
			if err = iter.push(u, i, max); err != nil {
				return
			}
		}
	}
	return
}

func (iter *Iterator) push(u *variable, min, max int) (err error) {
	for j, cs := range u.edges {
		if j >= min && j < max {
			// Update the incoming D2 constraints by using .dual to find them
			for _, c := range cs {
				// Since u has a value, all of its constraints are in consensus.
				// That means we can freely access their iterators!
				// In this case, all the iterators for the outgoing u.d2s have
				// values that are the counts (uint32) of them *and their dual*.

				i := c.place

				v := iter.variables[j]
				m, n := (i+1)%3, (i+2)%3

				place := i
				if v.node.Equal(c.quad[m]) {
					place = m
				} else if v.node.Equal(c.quad[n]) {
					place = n
				}

				neighbor := c.neighbors[place]
				neighbor.terms[i] = u.value

				item := c.iterator.Item()
				meta := item.UserMeta()
				if meta == UnaryPrefix {
					var p Permutation = i
					if place == m {
						p = place
					} else if place == n {
						p = place + 3
					}
					neighbor.prefix = assembleKey(BinaryPrefixes[p], true, u.value)
					neighbor.count, err = iter.unary.Get(p, u.value, iter.txn)
				} else {
					A, B := (neighbor.place+1)%3, (neighbor.place+2)%3
					neighbor.prefix = assembleKey(TernaryPrefixes[A], true, neighbor.terms[A], neighbor.terms[B])
					err = item.Value(func(val []byte) error {
						neighbor.count = binary.BigEndian.Uint32(val)
						return nil
					})
				}

				if err != nil {
					return
				}
			}

			// Clear the value, like I promised you earlier.
			w := iter.variables[j]
			w.value = NIL
			w.Sort()
		}
	}
	return
}
