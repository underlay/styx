package styx

import (
	"encoding/binary"
	"log"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

func indexElement(place uint8, quad *ld.Quad) []byte {
	var element string
	if place == 0 {
		element = quad.Subject.GetValue()
	} else if place == 1 {
		element = quad.Predicate.GetValue()
	} else if place == 2 {
		element = quad.Object.GetValue()
	} else {
		return nil
	}
	return []byte(element)
}

func populateKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) ([]byte, []byte) {
	var p, q []byte
	quad := dataset.Graphs[ref.Graph][ref.Index]
	if ref.P == "" {
		// p's slot is a URI or constant value
		place := (ref.Place + 1) % 3
		p = indexElement(place, quad)
	} else if i, has := as.deps[ref.P]; has {
		p = as.maps[i][ref.P].Value
	} else {
		log.Fatalln("Could not find p in assignment stack", ref.P)
	}

	if ref.Q == "" {
		place := (ref.Place + 2) % 3
		p = indexElement(place, quad)
	} else if i, has := as.deps[ref.Q]; has {
		q = as.maps[i][ref.Q].Value
	} else {
		log.Fatalln("Could not find q in assignment stack", ref.Q)
	}
	return p, q
}

func getValueKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	p, q := populateKey(ref, dataset, as)
	permutation := valuePrefixes[ref.Place]
	key := []byte{permutation, tab}
	key = append(key, p...)
	key = append(key, tab)
	key = append(key, q...)
	key = append(key, tab)
	return key
}

func getMajorKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	p, q := populateKey(ref, dataset, as)
	permutation := majorPrefixes[ref.Place]
	key := []byte{permutation, tab}
	key = append(key, p...)
	key = append(key, tab)
	key = append(key, q...)
	return key
}

func getMinorKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	p, q := populateKey(ref, dataset, as)
	permutation := minorPrefixes[ref.Place]
	key := []byte{permutation, tab}
	key = append(key, q...)
	key = append(key, tab)
	key = append(key, p...)
	return key
}

func countReferences(am AssignmentMap, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) error {
	counter := make([]byte, 8)
	for _, assignment := range am {
		var sum uint64
		for _, reference := range assignment.References {
			indexKey := getMajorKey(reference, dataset, as)
			// Or, equivalently:
			// indexKey := getMinorKey(reference, dataset, as)
			indexItem, err := txn.Get(indexKey)
			if err != nil {
				return err
			}
			counter, err = indexItem.ValueCopy(counter)
			if err != nil {
				return err
			}
			reference.Count = binary.BigEndian.Uint64(counter)
			sum = sum + reference.Count
		}
		assignment.Count = sum
	}
	return nil
}

func solveAssignment(id string, assignment *Assignment, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) (bool, error) {
	index := 0
	count := assignment.References[index].Count
	for i, reference := range assignment.References {
		if reference.Count < count {
			index = i
			count = reference.Count
		}
	}
	reference := assignment.References[index]
	prefix := getValueKey(reference, dataset, as)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = int(count)
	opts.PrefetchValues = true
	iter := txn.NewIterator(opts)
	defer iter.Close()
	var value []byte
	var key []byte
	var valueStart int
	var err error
	var passed bool
	secondValues := make([][]byte, len(assignment.References)-1)

	// This is a tricky one
	if assignment.Iterator == nil {
		iter.Seek(prefix)
	} else {
		iter.Seek(assignment.Iterator)
		iter.Next()
	}

	for ; iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		key = item.KeyCopy(key)
		value, err = item.ValueCopy(value)
		if err != nil {
			return false, err
		}

		// Strip value of the CID and graph label prefix.
		// This is why we used \n for the delimiter before the actual value,
		// since \t is used multiple times before it.
		for i, b := range value {
			if b == '\n' {
				valueStart = i
				break
			}
		}

		// Okay so each reference is an "expectation" of a triple.
		// It's NOT the case that every triple that matches the prefix has to match
		// Only that some do
		for i, reference := range assignment.References {
			if i == index {
				continue
			}
			secondPrefix := getValueKey(reference, dataset, as)
			opts := badger.DefaultIteratorOptions
			opts.PrefetchSize = int(reference.Count)
			opts.PrefetchValues = true
			secondIter := txn.NewIterator(opts)
			defer iter.Close()
			var secondValue []byte

			for secondIter.Seek(secondPrefix); secondIter.ValidForPrefix(secondPrefix); secondIter.Next() {
				secondItem := secondIter.Item()
				secondValue, err = secondItem.ValueCopy(secondValue)
				if err != nil {
					return false, err
				}

				var secondValueStart int
				for i, b := range secondValue {
					if b == '\n' {
						secondValueStart = i
						break
					}
				}

				if len(secondValue)-secondValueStart == len(value)-valueStart {
					passed = true
					for i, b := range value {
						if secondValue[i] != b {
							passed = false
							break
						}
					}
				}
				// If one of the references matches, we exit immediately
				// (aka the reference is satisfiable)
				if passed {
					break
				}
			}
			// The is the result of passed over all prefixes of reference
			if passed {
				j := i
				if j > index {
					j--
				}
				secondValues[j] = secondValue
			} else {
				break
			}
		}

		// This is the final thing for all references in value
		if passed {
			break
		} else {
			value = nil
		}
	}

	if value == nil {
		return false, nil
	}
	assignment.Value = value[valueStart:]
	assignment.Iterator = key
	return passed, nil
}

func solveAssignmentMap(am AssignmentMap, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) error {
	// The first thing we do is populate the assignment map with counter stats
	err := countReferences(am, as, dataset, txn)
	if err != nil {
		return err
	}

	// TODO: have a better sorting heuristic than iteration
	assignmentKeys := make([]string, len(am))
	var i int
	for id := range am {
		assignmentKeys[i] = id
		i++
	}
	var j int
	for j < len(am) {
		id := assignmentKeys[j]
		assignment := am[id]
		passed, err := solveAssignment(id, assignment, as, dataset, txn)
		if err != nil {
			return err
		} else if passed {
			// Do not have to backtrack :-)
			j++
		} else if j == 0 {

		} else {
			// Have to backtrack :-(
			j--
		}
	}
	return nil
}
