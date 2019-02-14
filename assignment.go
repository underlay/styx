package main

import (
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	badger "github.com/dgraph-io/badger"
)

// Past is history
type Past struct {
	Slice   []string
	Index   ReferenceMap
	Order   map[string]int
	Cursors CursorSet
	Indices map[string]map[int]int
}

func (past *Past) String() string {
	val := fmt.Sprintf("Past: %s\n", past.Cursors.String())
	for _, id := range past.Slice {
		val += fmt.Sprintf("  %s: %s\n", id, past.Index[id].String())
		pad := strings.Repeat(" ", len(id))
		for _, ref := range past.Index[id] {
			if ref.Cursor != nil {
				val += fmt.Sprintf("  %s  %s | %d\n", pad, string(ref.Cursor.Prefix), ref.Cursor.Count)
			}
		}
	}
	return val
}

// Push a dependency into the past
func (past *Past) Push(dep string, order int, refs ReferenceSet) {
	if past.Index == nil {
		past.Index = ReferenceMap{dep: refs}
	} else {
		past.Index[dep] = refs
	}

	if past.Order == nil {
		past.Order = map[string]int{dep: order}
	} else {
		past.Order[dep] = order
	}

	past.Slice = append(past.Slice, dep)
}

func (past *Past) insertIndex(id string, index int, value int) {
	if past.Indices == nil {
		past.Indices = map[string]map[int]int{id: map[int]int{index: value}}
	} else if past.Indices[id] == nil {
		past.Indices[id] = map[int]int{index: value}
	} else {
		past.Indices[id][index] = value
	}
}

// Sort interface for pastOrder
type pastOrder Past

func (po pastOrder) Len() int { return len(po.Slice) }
func (po pastOrder) Less(a, b int) bool {
	return po.Order[po.Slice[a]] > po.Order[po.Slice[b]]
}

func (po pastOrder) Swap(a, b int) {
	po.Slice[a], po.Slice[b] = po.Slice[b], po.Slice[a]
}

func (past *Past) sortOrder() {
	sort.Stable(pastOrder(*past))
}

// Sort interface for pastCursors
type pastCursors Past

func (pc pastCursors) Len() int { return len(pc.Cursors) }
func (pc pastCursors) Less(a, b int) bool {
	return pc.Cursors[a].Count < pc.Cursors[b].Count
}

func (pc pastCursors) Swap(a, b int) {
	idA, indexA := pc.Cursors[a].ID, pc.Cursors[a].Index
	idB, indexB := pc.Cursors[b].ID, pc.Cursors[b].Index
	pc.Indices[idA][indexA], pc.Indices[idB][indexB] = b, a
	pc.Cursors[a], pc.Cursors[b] = pc.Cursors[b], pc.Cursors[a]
}

func (past *Past) sortCursors() {
	sort.Stable(pastCursors(*past))
}

func (past *Past) setCursors() {
	past.Cursors = CursorSet{}
	for _, refs := range past.Index {
		past.Cursors = append(past.Cursors, refs.toCursorSet()...)
	}
	past.sortCursors()
}

// Dependencies are a slice of indices of assignments
// They're sorted high-to-low
type Dependencies []int

func (deps Dependencies) Len() int           { return len(deps) }
func (deps Dependencies) Less(a, b int) bool { return deps[a] < deps[b] }
func (deps Dependencies) Swap(a, b int)      { deps[a], deps[b] = deps[b], deps[a] }

// An Assignment is a setting of a variable to a value.
type Assignment struct {
	Value        uint64
	ValueRoot    []byte
	Sources      []*Source
	Constraint   ReferenceSet
	Present      ReferenceSet
	Past         *Past
	Future       ReferenceMap
	Static       CursorSet
	Dependencies Dependencies
}

func (a *Assignment) String() string {
	val := fmt.Sprintln("--- assignment ---")
	val += fmt.Sprintf("Value: %d\n", a.Value)
	val += fmt.Sprintf("ValueRoot: %s\n", string(a.ValueRoot))
	val += fmt.Sprintf("Sources: %s\n", sourcesToString(a.Sources))
	val += fmt.Sprintf("Constraint: %s\n", a.Constraint.String())
	val += fmt.Sprintf("Present: %s\n", a.Present.String())
	val += fmt.Sprintln("Future:")
	for id, refs := range a.Future {
		val += fmt.Sprintf("  %s: %s\n", id, refs.String())
	}
	if a.Past != nil {
		val += a.Past.String()
	}
	return val
}

// Seek to the next intersection
func (a *Assignment) Seek(value []byte) []byte {
	if value == nil {
		value = a.ValueRoot
	} else {
		value = Seek(a.Static, value)
	}

	fmt.Println("starting to seek from", string(value), a.Past.Slice)
	fmt.Println("past cursor set", a.Past.Cursors)
	if a.Past.Cursors.Len() > 0 {
		for {
			fmt.Println("top of loop with value", string(value))
			next := Seek(a.Past.Cursors, value)
			fmt.Println("got next value of", string(next))
			if next == nil {
				return nil
			} else if string(next) == string(value) {
				break
			} else {
				value = Seek(a.Static, next)
				if value == nil {
					return nil
				}
			}
		}
	}
	return value
}

// Next value
func (a *Assignment) Next() uint64 {
	value := Next(a.Static)
	if a.Past.Cursors.Len() > 0 {
		for {
			next := Seek(a.Past.Cursors, value)
			if next == nil {
				return 0
			} else if string(next) == string(value) {
				break
			} else {
				value = Seek(a.Static, next)
				if value == nil {
					return 0
				}
			}
		}
	}
	return binary.BigEndian.Uint64(value)
}

func (a *Assignment) setValueRoot(txn *badger.Txn) {
	// fmt.Println("attempting to set the value root")
	// fmt.Println(a.String())
	cursors := CursorSet{}
	if a.Present.Len() > 0 {
		for _, ref := range a.Present {
			ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
			permutation := (ref.Place + 1) % 3
			prefix := TriplePrefixes[permutation]
			m, n := ref.M.GetValue(nil), ref.N.GetValue(nil)
			ref.Cursor.Prefix = assembleKey(prefix, m, n, 0)
			cursors = append(cursors, ref.Cursor)
		}
	}

	if len(a.Future) > 0 {
		for _, refs := range a.Future {
			for _, ref := range refs {
				ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
				indexM, m := ref.M.(*Index)
				indexN, n := ref.N.(*Index)
				if m && !n {
					permutation := (ref.Place + 1) % 3
					prefix := MinorPrefixes[permutation]
					ref.Cursor.Prefix = assembleKey(prefix, indexM.GetId(), 0, 0)
				} else if !m && n {
					permutation := (ref.Place + 2) % 3
					prefix := MajorPrefixes[permutation]
					ref.Cursor.Prefix = assembleKey(prefix, indexN.GetId(), 0, 0)
				}
				cursors = append(cursors, ref.Cursor)
			}
		}
	}

	length := len(cursors)
	sort.Stable(cursors)

	if len(a.Past.Slice) > 0 {
		for _, id := range a.Past.Slice {
			for _, ref := range a.Past.Index[id] {
				ref.Cursor.Iterator = txn.NewIterator(iteratorOptions)
				indexM, m := ref.M.(*Index)
				indexN, n := ref.N.(*Index)
				if m && !n {
					permutation := (ref.Place + 1) % 3
					prefix := MinorPrefixes[permutation]
					ref.Cursor.Prefix = assembleKey(prefix, indexM.GetId(), 0, 0)
				} else if !m && n {
					permutation := (ref.Place + 2) % 3
					prefix := MajorPrefixes[permutation]
					ref.Cursor.Prefix = assembleKey(prefix, indexN.GetId(), 0, 0)
				}
				cursors = append(cursors, ref.Cursor)
			}
		}
	}

	valueRoot := Seek(cursors, nil)
	a.Static = cursors[:length]
	if valueRoot != nil {
		a.ValueRoot = valueRoot
	}
}

// An AssignmentMap is a map of string variable labels to assignments.
type AssignmentMap map[string]*Assignment

// This is re-used between Major, Minor, and Index keys
func getCount(key []byte, txn *badger.Txn) (uint64, error) {
	item, err := txn.Get(key)
	// KeyNotFound isn't necessarily an "error" - it just means the count is zero.
	if err == badger.ErrKeyNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	} else if count, err := item.ValueCopy(nil); err != nil {
		return 0, err
	} else {
		return binary.BigEndian.Uint64(count), nil
	}
}

func getAssignmentTree(codexMap *CodexMap, txn *badger.Txn) ([]string, AssignmentMap, error) {
	var err error
	var key []byte
	var major bool

	// Update the counts before sorting the codex map
	for _, codex := range codexMap.Index {
		codex.Count = 0
		for _, ref := range codex.Single {
			_, mIsIndex := ref.M.(*Index)
			_, nIsIndex := ref.N.(*Index)
			if mIsIndex && nIsIndex {
				key, major = ref.assembleSingleCountKey(nil, major)
			} else {
				return nil, nil, fmt.Errorf("Single reference has non-index M or N: %s", ref.String())
			}

			ref.Cursor = &Cursor{}
			ref.Cursor.Count, err = getCount(key, txn)
			if err != nil {
				return nil, nil, err
			} else if ref.Cursor.Count == 0 {
				return nil, nil, fmt.Errorf("Zero count single reference in codex: %s", ref.String())
			}
			codex.Count += ref.Cursor.Count
			codex.Norm += ref.Cursor.Count * ref.Cursor.Count
			codex.Length++
		}

		for _, refs := range codex.Double {
			for _, ref := range refs {
				var place uint8
				var index *Index
				ref.Cursor = &Cursor{}

				mIndex, mIsIndex := ref.M.(*Index)
				nIndex, nIsIndex := ref.N.(*Index)

				if mIsIndex && !nIsIndex {
					index = mIndex
					place = (ref.Place + 1) % 3
				} else if !mIsIndex && nIsIndex {
					index = nIndex
					place = (ref.Place + 2) % 3
				} else {
					return nil, nil, fmt.Errorf("Invalid double reference in codex: %s", ref.String())
				}

				if place == 0 {
					ref.Cursor.Count = index.GetSubject()
				} else if place == 1 {
					ref.Cursor.Count = index.GetPredicate()
				} else if place == 2 {
					ref.Cursor.Count = index.GetObject()
				}

				if ref.Cursor.Count == 0 {
					return nil, nil, fmt.Errorf("Double reference count of zero: %s", ref.String())
				}

				codex.Count += ref.Cursor.Count
				codex.Norm += ref.Cursor.Count * ref.Cursor.Count
				codex.Length++
			}
		}
	}

	// fmt.Println("sorted values:")
	// printCodexMap(c)
	// Now sort the codex map
	sort.Stable(codexMap)
	// fmt.Println("the codex map has been sorted", c.Slice)

	index := AssignmentMap{}
	indexMap := map[string]int{}
	for i, id := range codexMap.Slice {
		indexMap[id] = i

		codex := codexMap.Index[id]

		index[id] = &Assignment{
			Constraint: codex.Constraint,
			Present:    codex.Single,
			Past:       &Past{},
			Future:     ReferenceMap{},
		}

		deps := map[int]int{}
		past := index[id].Past
		for dep, refs := range codex.Double {
			if j, has := indexMap[dep]; has {
				past.Push(dep, j, refs)
				for k, ref := range refs {
					ref.Cursor.ID = dep
					ref.Cursor.Index = k
					past.insertIndex(dep, k, len(past.Cursors))
					past.Cursors = append(past.Cursors, ref.Cursor)
				}
				if j > deps[j] {
					deps[j] = j
				}
				for _, k := range index[dep].Dependencies {
					if j > deps[k] {
						deps[k] = j
					}
				}
			} else {
				index[id].Future[dep] = refs
			}
		}

		index[id].Past.sortOrder()

		// cursors := make(CursorSet, 0, cursorCount)
		// fmt.Println(len(index[id].Past.Slice), index[id].Past.Slice)
		// fmt.Println("and cursorCount", cursorCount)
		// for _, dep := range index[id].Past.Slice {
		// 	fmt.Println("trying for id", id)
		// }

		index[id].Dependencies = make([]int, 0, len(deps))
		for j := range deps {
			index[id].Dependencies = append(index[id].Dependencies, j)
		}
		sort.Sort(index[id].Dependencies)

		// fmt.Println("about to set value root for", id)
		index[id].setValueRoot(txn)
		if index[id].ValueRoot == nil {
			return nil, nil, fmt.Errorf("Assignment's static intersect is empty: %v", index[id])
		}
	}
	// return slice, index, nil
	// fmt.Println("returning slice", c.Slice)
	return codexMap.Slice, index, nil
}
