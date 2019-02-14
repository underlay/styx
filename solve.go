package main

import (
	"encoding/binary"
	"fmt"

	"github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"
)

func setValue(id string, value uint64, assignmentMap AssignmentMap) error {
	count := make([]byte, 8)
	assignmentMap[id].Value = value
	for dep := range assignmentMap[id].Future {
		for _, ref := range assignmentMap[dep].Past.Index[id] {
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

		assignmentMap[dep].Past.sortCursors()
	}
	return nil
}

// Solve the damn thing
func solveAssignment(id string, slice []string, assignmentMap AssignmentMap) (uint64, error) {
	l := assignmentMap[id].Dependencies.Len()
	fmt.Println("solving assignment", id)
	fmt.Println(assignmentMap[id].String())
	value := binary.BigEndian.Uint64(assignmentMap[id].Seek(nil))
	fmt.Println("seeking to value from", string(value))
	if value != 0 || l == 0 {
		return value, nil
	}

	oldValues := make(map[string]uint64, l)
	for _, j := range assignmentMap[id].Dependencies {
		id := slice[j]
		oldValues[id] = assignmentMap[id].Value
	}

	var i int
	for i < l {
		dep := slice[assignmentMap[id].Dependencies[i]]
		assignmentMap[dep].Value = assignmentMap[dep].Next()
		if assignmentMap[dep].Value == 0 {
			// reset everything including this index.
			// i goes to the next index
			i++
			for j := 0; j < i; j++ {
				dep := slice[assignmentMap[id].Dependencies[j]]
				err := setValue(dep, oldValues[dep], assignmentMap)
				if err != nil {
					return 0, err
				}
			}
			assignmentMap[id].Past.setCursors()
		} else {
			assignmentMap[id].Past.sortCursors()
			value := assignmentMap[id].Next()
			if value != 0 {
				return value, nil
			}
			// reset everything except this index.
			// i goes back to zero
			for j := 0; j < i; j++ {
				dep := slice[assignmentMap[id].Dependencies[j]]
				err := setValue(dep, oldValues[dep], assignmentMap)
				if err != nil {
					return 0, err
				}
			}
			assignmentMap[id].Past.setCursors()
			i = 0
		}
	}

	return 0, nil
}

func solveDataset(dataset *ld.RDFDataset, txn *badger.Txn) (AssignmentMap, error) {
	constants, codexMap, err := getInitalCodexMap(dataset, txn)
	// fmt.Println("Got the stuff")
	// fmt.Println(codexMap.String())
	// printCodexMap(codexMap)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Constants: %v\n", constants)

	slice, assignmentMap, err := getAssignmentTree(codexMap, txn)
	defer codexMap.Close()
	// fmt.Println("wow here's the slice we got", slice)
	printAssignments(slice, assignmentMap)
	if err != nil {
		return nil, err
	}

	for _, id := range slice {
		if value, err := solveAssignment(id, slice, assignmentMap); err != nil {
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
