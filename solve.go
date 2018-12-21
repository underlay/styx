package styx

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sort"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

func marshallNode(node ld.Node) []byte {
	var element string
	if iri, isIRI := node.(*ld.IRI); isIRI {
		element = marshallIRI(iri)
	} else if literal, isLiteral := node.(*ld.Literal); isLiteral {
		element = marshallLiteral(literal)
	} else {
		return nil
	}
	return []byte(element)
}

// func indexElement(permutation uint8, quad *ld.Quad) []byte {
// 	var element string
// 	if permutation == 0 {
// 		if iri, isIRI := quad.Subject.(*ld.IRI); isIRI {
// 			element = marshallIRI(iri)
// 		} else {
// 			log.Fatalln("Expected index subject to be IRI", quad.Subject)
// 		}
// 	} else if permutation == 1 {
// 		if iri, isIRI := quad.Predicate.(*ld.IRI); isIRI {
// 			element = marshallIRI(iri)
// 		} else {
// 			log.Fatalln("Expected index predicate to be IRI", quad.Predicate)
// 		}
// 	} else if permutation == 2 {
// 		if iri, isIRI := quad.Object.(*ld.IRI); isIRI {
// 			element = marshallIRI(iri)
// 		} else if literal, isLiteral := quad.Object.(*ld.Literal); isLiteral {
// 			element = marshallLiteral(literal)
// 		} else {
// 			log.Fatalln("Expected index object to be IRI or literal", quad.Subject)
// 		}
// 	} else {
// 		return nil
// 	}
// 	return []byte(element)
// }

func assembleReferenceKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) ([]byte, []byte) {
	var m, n []byte
	if M, isBlank := ref.M.(*ld.BlankNode); isBlank {
		id := M.Attribute
		index := as.deps[id]
		m = as.maps[index][id].Value
	} else {
		m = marshallNode(ref.M)
	}

	if N, isBlank := ref.N.(*ld.BlankNode); isBlank {
		id := N.Attribute
		index := as.deps[id]
		n = as.maps[index][id].Value
	} else {
		n = marshallNode(ref.N)
	}

	return m, n
}

func getValueKeyPrefix(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	m, n := assembleReferenceKey(ref, dataset, as)
	prefix := ValuePrefixes[ref.Permutation]
	return assembleKey(prefix, m, n, nil)
}

func getMajorKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	m, n := assembleReferenceKey(ref, dataset, as)
	prefix := MajorPrefixes[ref.Permutation]
	return assembleKey(prefix, m, n, nil)
}

func getMinorKey(ref Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	m, n := assembleReferenceKey(ref, dataset, as)
	prefix := MinorPrefixes[ref.Permutation]
	// sketchy af
	return assembleKey(prefix, n, m, nil)
}

// SortedReferences implements sort.Interface by Reference.Count
type SortedReferences []Reference

func (sr SortedReferences) Len() int           { return len(sr) }
func (sr SortedReferences) Less(i, j int) bool { return sr[i].Count < sr[j].Count }
func (sr SortedReferences) Swap(i, j int)      { sr[i], sr[j] = sr[j], sr[i] }

// SortedAssignments implements sort.Interface by Assignment.Count
type SortedAssignments struct {
	Assignments []*Assignment
	Ids         []string
}

func (sa SortedAssignments) Len() int { return len(sa.Ids) }
func (sa SortedAssignments) Less(i, j int) bool {
	return sa.Assignments[i].Count < sa.Assignments[j].Count
}
func (sa SortedAssignments) Swap(i, j int) {
	sa.Assignments[i], sa.Assignments[j] = sa.Assignments[j], sa.Assignments[i]
	sa.Ids[i], sa.Ids[j] = sa.Ids[j], sa.Ids[i]
}

func countReferences(am AssignmentMap, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) error {
	counter := make([]byte, 8)
	var major bool
	for _, assignment := range am {
		var sum uint64
		for _, reference := range assignment.References {
			// This is really over the top, but just for a sense of balance:
			// alternate between getting the count from the major and minor indices.
			var indexKey []byte
			if major {
				indexKey = getMajorKey(reference, dataset, as)
			} else {
				indexKey = getMinorKey(reference, dataset, as)
			}
			major = !major

			indexItem, err := txn.Get(indexKey)
			if err != nil {
				return err
			}

			counter, err = indexItem.ValueCopy(counter)
			if err != nil {
				return err
			}

			reference.Count = binary.BigEndian.Uint64(counter)
			sum += reference.Count
		}

		// Now that we've populated Reference.Count, it's time to sort the
		// references in the assignment map
		sort.Sort(SortedReferences(assignment.References))

		// Cool! Now we just populate assignment.Count with the sum
		// that we were keeping track of the whole time and we're done!
		assignment.Count = sum
	}
	return nil
}

func solveAssignmentMap(am AssignmentMap, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) (bool, error) {
	// The first thing we do is populate the assignment map with counter stats
	err := countReferences(am, as, dataset, txn)
	fmt.Println("counted references", err)
	if err != nil {
		return false, err
	}

	// Good job. The assignment map is now populated.

	// Create the sortedAssignment struct that we'll sort in a second
	sortedAssignments := SortedAssignments{
		Assignments: make([]*Assignment, len(am)),
		Ids:         make([]string, len(am)),
	}

	// Populate it in whatever order range decides to give us
	var i int
	for id, assignment := range am {
		sortedAssignments.Assignments[i] = assignment
		sortedAssignments.Ids[i] = id
		i++
	}

	// Now sort!
	sort.Sort(sortedAssignments)

	var j int
	for j < len(am) {
		fmt.Println("iterating with j", j)
		id, assignment := sortedAssignments.Ids[j], sortedAssignments.Assignments[j]
		value, err := solveAssignment(id, assignment, as, dataset, txn)
		fmt.Println("solving assignment", value, err)
		if err != nil {
			return false, err
		} else if value != nil {
			// Do not have to backtrack :-)
			fmt.Println("do not have to backtrack! :-)")
			j++
		} else if j == 0 {
			fmt.Println("failed to resolve")
			b, _ := json.MarshalIndent(assignment, "", "  ")
			fmt.Println(string(b))
			return false, nil
		} else {
			// Have to backtrack :-(
			fmt.Println("have to backtrack! :-(")
			j--
		}
	}
	return true, nil
}

func solveAssignment(id string, assignment *Assignment, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) ([]byte, error) {
	if assignment.References == nil || len(assignment.References) == 0 {
		log.Fatalln("Why is assignment.References empty?")
	}

	joins := make([][]byte, len(assignment.References)-1)
	pivot := getValueKeyPrefix(assignment.References[0], dataset, as)
	for i := 1; i < len(assignment.References); i++ {
		reference := assignment.References[i]
		joins[i-1] = getValueKeyPrefix(reference, dataset, as)
	}

	value, iterator, sources, err := join(pivot, assignment.Iterator, int(assignment.References[0].Count), joins, txn)
	if err != nil {
		return nil, err
	}

	assignment.Value = value
	assignment.Iterator = iterator
	assignment.Sources = sources

	fmt.Println("got value", value, string(value))

	return value, nil
}
