package styx

import (
	"encoding/binary"
	"log"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

func indexElement(place uint8, quad *ld.Quad) string {
	if place == 0 {
		return quad.Subject.GetValue()
	} else if place == 1 {
		return quad.Predicate.GetValue()
	} else if place == 2 {
		return quad.Object.GetValue()
	} else {
		return ""
	}
}

func populateKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) (string, string) {
	var p, q string
	quad := dataset.Graphs[ref.Graph][ref.Index]
	if ref.P == "" {
		// p's slot is a URI or constant value
		place := (ref.Place + 1) % 3
		p = indexElement(place, quad)
	} else if i, has := as.deps[ref.P]; has {
		p = as.maps[i][p].Value
	} else {
		log.Fatalln("Could not find p in assignment stack", ref.P)
	}

	if ref.Q == "" {
		place := (ref.Place + 2) % 3
		p = indexElement(place, quad)
	} else if i, has := as.deps[ref.Q]; has {
		q = as.maps[i][q].Value
	} else {
		log.Fatalln("Could not find q in assignment stack", ref.Q)
	}
	return p, q
}

func getValueKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	p, q := populateKey(ref, dataset, as)
	permutation := valuePrefixes[ref.Place]
	key := []byte{permutation, tab}
	key = append(key, []byte(p)...)
	key = append(key, tab)
	key = append(key, []byte(q)...)
	key = append(key, tab)
	return key
}

func getMajorKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	p, q := populateKey(ref, dataset, as)
	permutation := majorPrefixes[ref.Place]
	key := []byte{permutation, tab}
	key = append(key, []byte(p)...)
	key = append(key, tab)
	key = append(key, []byte(q)...)
	return key
}

func getMinorKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	p, q := populateKey(ref, dataset, as)
	permutation := minorPrefixes[ref.Place]
	key := []byte{permutation, tab}
	key = append(key, []byte(q)...)
	key = append(key, tab)
	key = append(key, []byte(p)...)
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

func solveAssignment(id string, assignment *Assignment, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) error {
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
	var err error
	for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
		item := iter.Item()
		value, err = item.ValueCopy(value)
		if err != nil {
			return err
		}

		// Strip value of the CID and graph label prefix
		for i, b := range value {
			if b == '\n' {
				value = value[i+1:]
				break
			}
		}

		// Now value is really actually the value.
		// for i, reference := range assignment.References {
		// 	if i == index {
		// 		continue
		// 	}
		// }
	}
	if value == nil {
		// !FAILURE :-o
		// back up the track
	} else {

	}

	return nil
}

func solveAssignmentMap(am AssignmentMap, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) error {
	// The first thing we do is populate the assignment map with counter stats
	err := countReferences(am, as, dataset, txn)
	if err != nil {
		return err
	}

	// TODO: have a better sorting heuristic than iteration
	for id, assignment := range am {
		err = solveAssignment(id, assignment, as, dataset, txn)
		if err != nil {
			return err
		}
	}
	return nil
}

func query(dataset *ld.RDFDataset, db *badger.DB, cb func(AssignmentStack) error) error {
	as := getAssignmentStack(dataset)
	return db.View(func(txn *badger.Txn) error {
		for _, am := range as.maps {
			solveAssignmentMap(am, as, dataset, txn)
		}
		return cb(as)
	})
}
