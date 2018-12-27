package styx

import (
	"encoding/binary"
	"sort"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

// SortedReferences implements sort.Interface by Reference.Count
type SortedReferences []*Reference

func (sr SortedReferences) Len() int           { return len(sr) }
func (sr SortedReferences) Less(i, j int) bool { return sr[i].Count < sr[j].Count }
func (sr SortedReferences) Swap(i, j int)      { sr[i], sr[j] = sr[j], sr[i] }

func countReference(reference *Reference, major bool, counter []byte, txn *badger.Txn, dataset *ld.RDFDataset, as AssignmentStack) (uint64, error) {
	// This is really over the top, but just for a sense of balance:
	// alternate between getting the count from the major and minor indices.
	var indexKey []byte
	if major {
		indexKey = getMajorKey(reference, dataset, as)
	} else {
		indexKey = getMinorKey(reference, dataset, as)
	}

	indexItem, err := txn.Get(indexKey)
	if err != nil {
		return 0, err
	}

	counter, err = indexItem.ValueCopy(counter)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(counter), nil
}

func countReferences(am AssignmentMap, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) (string, *Reference, error) {
	counter := make([]byte, 8)
	var major bool
	for id, assignment := range am {
		var sum uint64
		for _, reference := range assignment.References {
			count, err := countReference(reference, major, counter, txn, dataset, as)
			if err == badger.ErrKeyNotFound {
				return id, reference, nil
			} else if err != nil {
				return "", nil, err
			}
			major = !major
			reference.Count = count
			sum += count
		}

		// Now that we've populated Reference.Count, it's time to sort the
		// references in the assignment map
		sort.Sort(SortedReferences(assignment.References))

		// Cool! Now we just populate assignment.Count with the sum
		// that we were keeping track of the whole time and we're done!
		assignment.Count = sum
	}
	return "", nil, nil
}

func getMajorKey(ref *Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	m, n := assembleReferenceKey(ref, dataset, as)
	prefix := MajorPrefixes[ref.Permutation]
	return assembleKey(prefix, m, n, nil)
}

func getMinorKey(ref *Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	m, n := assembleReferenceKey(ref, dataset, as)
	prefix := MinorPrefixes[ref.Permutation]
	// sketchy af
	return assembleKey(prefix, n, m, nil)
}
