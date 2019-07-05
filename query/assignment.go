package query

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v2"

	types "github.com/underlay/styx/types"
)

// HasValue is either a string representing a variable reference,
// or an Index representing an absolute value from the database
type HasValue interface {
	GetValue(param interface{}) uint64
}

// An Assignment is a setting of a variable to a value.
type Assignment struct {
	Value        [8]byte
	ValueRoot    [8]byte
	Sources      types.Sources
	Constraint   ReferenceSet
	Past         ReferenceMap
	Present      ReferenceSet
	Future       ReferenceMap
	Static       CursorSet
	Dynamic      CursorSet
	Dependencies []int
	Dependents   []int
}

func (a *Assignment) String() string {
	val := fmt.Sprintln("--- assignment ---")
	val += fmt.Sprintf("Value: %d\n", a.Value)
	val += fmt.Sprintf("ValueRoot: %v\n", a.ValueRoot)
	val += fmt.Sprintf("Sources: %s\n", a.Sources.String())
	val += fmt.Sprintf("Constraint: %s\n", a.Constraint.String())
	val += fmt.Sprintf("Present: %s\n", a.Present.String())
	val += fmt.Sprintln("Future:")
	for id, refs := range a.Future {
		val += fmt.Sprintf("  %s: %s\n", id, refs.String())
	}
	val += fmt.Sprintln("Past:")
	for id, refs := range a.Past {
		val += fmt.Sprintf("  %s: %s\n", id, refs.String())
	}
	return val
}

// Seek to the next intersection
func (a *Assignment) Seek(value []byte) []byte {
	if value == nil {
		value = a.ValueRoot[:]
	} else if value = a.Static.Seek(value); value == nil {
		return nil
	}

	if a.Dynamic.Len() > 0 {
		for {
			next := a.Dynamic.Seek(value)
			if next == nil {
				return nil
			} else if bytes.Equal(next, value) {
				break
			} else {
				value = a.Static.Seek(next)
				if value == nil {
					return nil
				}
			}
		}
	}
	return value
}

// Next value
func (a *Assignment) Next() []byte {
	value := a.Static.Next()
	if a.Dynamic.Len() > 0 {
		for {
			next := a.Dynamic.Seek(value)
			if next == nil {
				return nil
			} else if bytes.Equal(next, value) {
				break
			} else {
				value = a.Static.Seek(next)
				if value == nil {
					return nil
				}
			}
		}
	}
	return value
}

// An AssignmentMap is a map of string variable labels to assignments.
type AssignmentMap struct {
	Index map[string]*Assignment
	Slice []string
	Map   map[string]int
}

func (assignmentMap *AssignmentMap) String() string {
	var s string
	for _, id := range assignmentMap.Slice {
		a := assignmentMap.Index[id]
		s += fmt.Sprintf("id: %s\n", id)
		s += fmt.Sprintln(a.String())
	}
	return s
}

func getAssignmentMap(codexMap *CodexMap, txn *badger.Txn) (*AssignmentMap, error) {
	// Update the counts before sorting the codex map
	err := codexMap.Initialize(txn)
	if err != nil {
		return nil, err
	}

	// Now sort the codex map
	sort.Stable(codexMap)

	inverse := map[string]int{}
	for i, id := range codexMap.Slice {
		inverse[id] = i
	}

	index := map[string]*Assignment{}
	indexMap := map[string]int{} // temp dict, only used for sorting here
	dependentMaps := map[string]map[int]int{}
	for i, id := range codexMap.Slice {
		indexMap[id] = i

		codex := codexMap.Index[id]

		index[id] = &Assignment{
			Constraint: codex.Constraint,
			Present:    codex.Single,
			Past:       ReferenceMap{},
			Future:     ReferenceMap{},
			Static:     CursorSet{},
			Dynamic:    CursorSet{},
		}

		copy(index[id].ValueRoot[:], codex.Root)

		// deps is a flattened map of index[id]'s "dependency subtree",
		// mapping the dependency's index (from codexMap.Slice) to the index
		// of their "highest" (most responsible) dependent.
		// So if a <-- b, b <-- c, and also a <-- c
		// ("c depends on a and b, b depends on a"),
		// then deps[indexMap[a]] = deps[indexMap[c]]
		dependencies := map[int]int{}

		for _, ref := range codex.Single {
			index[id].Static = append(index[id].Static, ref.Cursor)
		}

		for dep, refs := range codex.Double {
			if j, has := indexMap[dep]; has {
				// j is the index of dep in codexMap.Slice
				// past.Push(dep, j, refs)

				// Add the refs to Past under dep
				index[id].Past[dep] = refs

				for k, ref := range refs {
					ref.Cursor.ID = dep
					ref.Cursor.Index = k
					index[id].Dynamic = append(index[id].Dynamic, ref.Cursor)
				}

				if j > dependencies[j] {
					dependencies[j] = j
					if dependents, has := dependentMaps[dep]; has {
						dependents[i] = j
					} else {
						dependentMaps[dep] = map[int]int{i: j}
					}
				}

				for _, k := range index[dep].Dependencies {
					if j > dependencies[k] {
						dependencies[k] = j
						if dependents, has := dependentMaps[dep]; has {
							dependents[i] = j
						} else {
							dependentMaps[dep] = map[int]int{i: j}
						}
					}
				}
			} else {
				// Add the refs to Future under dep
				index[id].Future[dep] = refs
				for _, ref := range refs {
					index[id].Static = append(index[id].Static, ref.Cursor)
				}
			}
		}

		sort.Stable(index[id].Static)

		index[id].Dependencies = make([]int, 0, len(dependencies))
		for j := range dependencies {
			index[id].Dependencies = append(index[id].Dependencies, j)
		}

		sort.Ints(index[id].Dependencies)
	}

	for id, assignment := range index {
		assignment.Dependents = make([]int, 0, len(dependentMaps[id]))
		for j := range dependentMaps[id] {
			assignment.Dependents = append(assignment.Dependents, j)
		}
		sort.Ints(assignment.Dependents)
	}

	return &AssignmentMap{Index: index, Slice: codexMap.Slice, Map: inverse}, nil
}

// A Variable is a string with a GetValue method
type Variable string

// GetValue satisfies the HasValue interface for variables by looking up the
// variable's value in the assignmentMap.
func (variable Variable) GetValue(param interface{}) uint64 {
	if assignmentMap, isAssignmentMap := param.(*AssignmentMap); isAssignmentMap {
		id := string(variable)
		if assignment, has := assignmentMap.Index[id]; has {
			return binary.BigEndian.Uint64(assignment.Value[:])
		}
	}
	return 0
}
