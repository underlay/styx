package styx

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
				ref.Cursor.Prefix = assembleKey(prefix, value, []byte(ref.N.GetValue()), nil)
			} else if ld.IsBlankNode(ref.N) {
				ref.Cursor.Prefix = assembleKey(prefix, []byte(ref.M.GetValue()), value, nil)
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
func solve(id string, slice []string, index map[string]*Assignment) ([]byte, error) {
	l := index[id].Dependencies.Len()

	value := index[id].Seek(nil)
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

func solveDataset(dataset *ld.RDFDataset, txn *badger.Txn) error {
	constants, codexMap, err := getInitalCodexMap(dataset)
	fmt.Println("Got the stuff")
	// fmt.Println(codexMap.String())
	printCodexMap(codexMap)
	if err != nil {
		return err
	}

	fmt.Printf("Constants: %v\n", constants)

	slice, index, err := codexMap.getAssignmentTree(txn)
	defer closeAssignments(index)
	printAssignments(slice, index)
	if err != nil {
		return err
	}

	for _, id := range slice {
		if value, err := solve(id, slice, index); err != nil {
			return err
		} else if value == nil {
			return fmt.Errorf("Could not satisfy value: %s", id)
		} else if err := setValue(id, value, index); err != nil {
			return err
		}
	}

	return nil
}
