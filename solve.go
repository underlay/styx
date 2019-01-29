package main

import (
	"encoding/binary"
	fmt "fmt"

	"github.com/dgraph-io/badger"
	ld "github.com/piprate/json-gold/ld"
)

func setValue(id string, value []byte, index map[string]*Assignment) error {
	count := make([]byte, 8)
	index[id].Value = value
	for dep := range index[id].Future {
		for _, ref := range index[dep].Past.Index[id] {
			permutation := (ref.Permutation + 1) % 3
			prefix := ValuePrefixes[permutation]
			if ld.IsBlankNode(ref.M) {
				n := marshalReferenceNode(ref.N, index)
				ref.Cursor.Prefix = assembleKey(prefix, value, n, nil)
			} else if ld.IsBlankNode(ref.N) {
				m := marshalReferenceNode(ref.M, index)
				ref.Cursor.Prefix = assembleKey(prefix, m, value, nil)
			}
			item := ref.Dual.Cursor.Iterator.Item()
			count, err := item.ValueCopy(count)
			if err != nil {
				return err
			}
			ref.Cursor.Count = binary.BigEndian.Uint64(count)
		}

		index[dep].Past.sortCursors()
	}
	return nil
}

// Solve the damn thing
func solveAssignment(id string, slice []string, index map[string]*Assignment) ([]byte, error) {
	l := index[id].Dependencies.Len()
	fmt.Println("solving assignment", id)
	fmt.Println(index[id].String())
	value := index[id].Seek(nil)
	fmt.Println("seeking to value from", string(value))
	if value != nil || l == 0 {
		return value, nil
	}

	oldValues := make(map[string][]byte, l)
	for _, j := range index[id].Dependencies {
		id := slice[j]
		oldValues[id] = index[id].Value
	}

	var i int
	for i < l {
		dep := slice[index[id].Dependencies[i]]
		index[dep].Value = index[dep].Next()
		if index[dep].Value == nil {
			// reset everything including this index.
			// i goes to the next index
			i++
			for j := 0; j < i; j++ {
				dep := slice[index[id].Dependencies[j]]
				err := setValue(dep, oldValues[dep], index)
				if err != nil {
					return nil, err
				}
			}
			index[id].Past.setCursors()
		} else {
			index[id].Past.sortCursors()
			value := index[id].Next()
			if value != nil {
				return value, nil
			}
			// reset everything except this index.
			// i goes back to zero
			for j := 0; j < i; j++ {
				dep := slice[index[id].Dependencies[j]]
				err := setValue(dep, oldValues[dep], index)
				if err != nil {
					return nil, err
				}
			}
			index[id].Past.setCursors()
			i = 0
		}
	}

	return nil, nil
}

func solveDataset(dataset *ld.RDFDataset, txn *badger.Txn) (map[string]*Assignment, error) {
	constants, codexMap, err := getInitalCodexMap(dataset)
	// fmt.Println("Got the stuff")
	// fmt.Println(codexMap.String())
	// printCodexMap(codexMap)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Constants: %v\n", constants)

	slice, index, err := codexMap.getAssignmentTree(txn)
	defer codexMap.Close()
	// fmt.Println("wow here's the slice we got", slice)
	printAssignments(slice, index)
	if err != nil {
		return nil, err
	}

	for _, id := range slice {
		if value, err := solveAssignment(id, slice, index); err != nil {
			return nil, err
		} else if value == nil {
			return index, fmt.Errorf("Could not satisfy value: %s", id)
		} else if err := setValue(id, value, index); err != nil {
			return nil, err
		} else {
			fmt.Printf("set %s to %s\n", id, string(value))
		}
	}

	// printAssignments(slice, index)
	return index, nil
}
