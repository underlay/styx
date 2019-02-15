package main

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"
)

func setValue(id string, value uint64, assignmentMap *AssignmentMap) error {
	count := make([]byte, 8)
	assignment := assignmentMap.Index[id]
	assignment.Value = value
	for dep := range assignment.Future {
		past := assignmentMap.Index[dep].Past
		for _, ref := range past.Index[id] {
			place := (ref.Place + 1) % 3
			prefix := TriplePrefixes[place]
			mIndex, mIsIndex := ref.M.(*Index)
			nIndex, nIsIndex := ref.N.(*Index)
			if mIsIndex {
				ref.Cursor.Prefix = assembleKey(prefix, mIndex.GetId(), value, 0)
			} else if nIsIndex {
				ref.Cursor.Prefix = assembleKey(prefix, value, nIndex.GetId(), 0)
			}
			item := ref.Dual.Cursor.Iterator.Item()
			count, err := item.ValueCopy(count)
			if err != nil {
				return err
			}
			ref.Cursor.Count = binary.BigEndian.Uint64(count)
		}
		past.sortCursors()
	}
	return nil
}

// Solve the damn thing
func solveAssignment(id string, assignmentMap *AssignmentMap) (uint64, error) {
	assignment := assignmentMap.Index[id]
	l := len(assignment.Dependencies)
	fmt.Println("solving assignment", id)
	fmt.Println(assignment.String())
	value := binary.BigEndian.Uint64(assignment.Seek(nil))
	fmt.Println("seeking to value from", string(value))
	if value != 0 || l == 0 {
		return value, nil
	}

	oldValues := make(map[string]uint64, l)
	for _, j := range assignment.Dependencies {
		id := assignmentMap.Slice[j]
		oldValues[id] = assignment.Value
	}

	var i int
	for i < l {
		dep := assignmentMap.Slice[assignment.Dependencies[i]]
		assignmentMap.Index[dep].Value = assignmentMap.Index[dep].Next()
		if assignmentMap.Index[dep].Value == 0 {
			// reset everything including this index.
			// i goes to the next index
			i++
			for j := 0; j < i; j++ {
				dep := assignmentMap.Slice[assignment.Dependencies[j]]
				err := setValue(dep, oldValues[dep], assignmentMap)
				if err != nil {
					return 0, err
				}
			}
			assignment.Past.setCursors()
		} else {
			assignment.Past.sortCursors()
			value := assignment.Next()
			if value != 0 {
				return value, nil
			}
			// reset everything except this index.
			// i goes back to zero
			for j := 0; j < i; j++ {
				dep := assignmentMap.Slice[assignment.Dependencies[j]]
				err := setValue(dep, oldValues[dep], assignmentMap)
				if err != nil {
					return 0, err
				}
			}
			assignment.Past.setCursors()
			i = 0
		}
	}

	return 0, nil
}

func solveDataset(dataset *ld.RDFDataset, txn *badger.Txn) (*AssignmentMap, error) {
	constants, codexMap, err := getInitalCodexMap(dataset, txn)
	// fmt.Println("Got the stuff")
	// fmt.Println(codexMap.String())
	// printCodexMap(codexMap)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Constants: %v\n", constants)

	assignmentMap, err := getAssignmentTree(codexMap, txn)
	defer codexMap.Close()
	// fmt.Println("wow here's the slice we got", slice)
	printAssignments(assignmentMap)
	if err != nil {
		return nil, err
	}

	for _, id := range assignmentMap.Slice {
		if value, err := solveAssignment(id, assignmentMap); err != nil {
			return nil, err
		} else if value == 0 {
			return assignmentMap, fmt.Errorf("Could not satisfy value: %s", id)
		} else if err := setValue(id, value, assignmentMap); err != nil {
			return nil, err
		} else {
			fmt.Printf("set %s to %s\n", id, string(value))
		}
	}

	// printAssignments(slice, assignmentMap)
	return assignmentMap, nil
}
