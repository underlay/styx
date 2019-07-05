package query

import (
	"encoding/binary"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

// SolveGraph solves the graph
func SolveGraph(graph string, quads []*ld.Quad, txn *badger.Txn) (*AssignmentMap, error) {
	_, codexMap, err := getInitalCodexMap(graph, quads, txn)
	defer codexMap.Close()
	if err != nil {
		return nil, err
	}

	assignmentMap, err := getAssignmentMap(codexMap, txn)

	if err != nil {
		return nil, err
	}

	for i, id := range assignmentMap.Slice {

		if value, err := solveAssignment(i, id, assignmentMap); err != nil {
			return nil, err
		} else if value == nil {
			return assignmentMap, fmt.Errorf("Could not satisfy value: %s", id)
		} else if err := setValue(id, value, assignmentMap); err != nil {
			return nil, err
		}
	}

	return assignmentMap, nil
}

// Solve the damn thing
func solveAssignment(index int, id string, assignmentMap *AssignmentMap) ([]byte, error) {
	assignment := assignmentMap.Index[id]

	l := len(assignment.Dependencies)

	value := assignment.Seek(nil)
	if value != nil || l == 0 {
		return value, nil
	}

	oldValues := make(map[string][]byte, l)
	oldCounts := map[[2]string][]uint64{}
	oldPrefixes := map[[2]string][][]byte{}
	for _, j := range assignment.Dependencies {
		dep := assignmentMap.Slice[j]
		dependency := assignmentMap.Index[dep]
		oldValues[dep] = dependency.Value[:]
		for f, refs := range dependency.Future {
			if assignmentMap.Map[f] > index {
				continue
			}
			counts := make([]uint64, len(refs))
			prefixes := make([][]byte, len(refs))
			for k, ref := range refs {
				counts[k] = ref.Dual.Cursor.Count
				prefixes[k] = ref.Dual.Cursor.Prefix
			}
			key := [2]string{dep, f}
			oldCounts[key] = counts
			oldPrefixes[key] = prefixes
		}
	}

	var i = l - 1
	for i >= 0 {

		k := assignment.Dependencies[i]
		dep := assignmentMap.Slice[k]
		results, err := inventFuture(index, dep, assignmentMap)
		if err != nil {
			return nil, err
		} else if results == nil {
			// reset everything *including* this index.
			for j := i; j < l; j++ {
				k := assignment.Dependencies[j]
				dep := assignmentMap.Slice[k]
				resetValue(index, dep, oldValues, oldCounts, oldPrefixes, assignmentMap)
			}
			// i goes to the next ("previous") index.
			i--
		} else {
			sort.Stable(assignment.Dynamic)
			value := assignment.Next()
			if value != nil {
				for j, result := range results {
					k := assignmentMap.Index[dep].Dependents[j]
					intern := assignmentMap.Slice[k]
					dependent := assignmentMap.Index[intern]
					copy(dependent.Value[:], result)

					for f, refs := range dependent.Future {
						if assignmentMap.Map[f] > index {
							for _, ref := range refs {
								setRef(ref, result)
							}
						}
					}
				}
				return value, nil
			}

			// reset everything *except* this index.
			// i goes back to the beginning
			for j := i + 1; j < l; j++ {
				k := assignment.Dependencies[j]
				dep := assignmentMap.Slice[k]
				resetValue(index, dep, oldValues, oldCounts, oldPrefixes, assignmentMap)
			}
			sort.Stable(assignment.Dynamic)
			i = l - 1
		}
	}

	return nil, nil
}

func inventFuture(index int, dep string, assignmentMap *AssignmentMap) ([][]byte, error) {
	dependency := assignmentMap.Index[dep]
	value := dependency.Next()
	if value == nil {
		return nil, nil
	}

	err := setInterim(index, dep, value, assignmentMap)
	if err != nil {
		return nil, err
	}

	results := [][]byte{}

	for _, j := range dependency.Dependents {
		if j >= index {
			break
		}

		intern := assignmentMap.Slice[j]
		sort.Stable(assignmentMap.Index[intern].Dynamic)

		next := assignmentMap.Index[intern].Seek(nil)
		if next == nil {
			return nil, nil
		}

		err := setInterim(index, intern, next, assignmentMap)
		if err != nil {
			return nil, err
		}

		results = append(results, next)
	}

	return results, nil
}

func setInterim(index int, dep string, value []byte, assignmentMap *AssignmentMap) error {
	dependency := assignmentMap.Index[dep]

	for f, refs := range dependency.Future {
		if assignmentMap.Map[f] > index {
			continue
		}

		for _, ref := range refs {
			err := setRef(ref, value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func resetValue(
	index int,
	dep string,
	oldValues map[string][]byte,
	oldCounts map[[2]string][]uint64,
	oldPrefixes map[[2]string][][]byte,
	assignmentMap *AssignmentMap,
) {
	value := oldValues[dep]
	dependency := assignmentMap.Index[dep]

	sort.Stable(dependency.Dynamic)

	for _, cursor := range dependency.Static {
		key := append(cursor.Prefix, value...)
		cursor.Iterator.Seek(key)
	}

	for _, cursor := range dependency.Dynamic {
		key := append(cursor.Prefix, value...)
		cursor.Iterator.Seek(key)
	}

	for f, refs := range dependency.Future {
		if assignmentMap.Map[f] > index {
			continue
		}
		key := [2]string{dep, f}
		counts := oldCounts[key]
		prefixes := oldPrefixes[key]
		for i, ref := range refs {
			ref.Dual.Cursor.Count = counts[i]
			ref.Dual.Cursor.Prefix = prefixes[i]
		}
	}
}

func setRef(ref *Reference, value []byte) error {
	dual := ref.Dual
	place := (dual.Place + 1) % 3
	prefix := types.TriplePrefixes[place]

	_, mIsIndex := dual.M.(*types.Index)
	_, nIsIndex := dual.N.(*types.Index)
	if mIsIndex && !nIsIndex {
		dual.Cursor.Prefix = types.AssembleKey(prefix, dual.BytesM, value, nil)
	} else if !mIsIndex && nIsIndex {
		dual.Cursor.Prefix = types.AssembleKey(prefix, value, dual.BytesN, nil)
	}

	item := ref.Cursor.Iterator.Item()
	count, err := item.ValueCopy(nil)
	if err != nil {
		return err
	}

	dual.Cursor.Count = binary.BigEndian.Uint64(count)
	return nil
}

func setValue(id string, value []byte, assignmentMap *AssignmentMap) error {
	assignment := assignmentMap.Index[id]
	copy(assignment.Value[:], value)
	for _, refs := range assignment.Future {
		for _, ref := range refs {
			err := setRef(ref, value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
