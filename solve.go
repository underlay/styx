package styx

import (
	"encoding/json"
	"errors"
	"fmt"
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

func assembleReferenceKey(ref *Reference, dataset *ld.RDFDataset, as AssignmentStack) ([]byte, []byte) {
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

func getValueKeyPrefix(ref *Reference, dataset *ld.RDFDataset, as AssignmentStack) []byte {
	m, n := assembleReferenceKey(ref, dataset, as)
	prefix := ValuePrefixes[ref.Permutation]
	return assembleKey(prefix, m, n, nil)
}

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

func solveAssignmentMap(am AssignmentMap, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) (bool, error) {
	length := len(am)
	// Create the sortedAssignment struct that we'll sort in a second
	sortedAssignments := SortedAssignments{
		Assignments: make([]*Assignment, length),
		Ids:         make([]string, length),
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
	set := map[string]*Assignment{}
	for j < length {
		fmt.Println("iterating with j", j)
		id, assignment := sortedAssignments.Ids[j], sortedAssignments.Assignments[j]
		set[id] = assignment
		value, err := solveAssignment(set, id, as, dataset, txn)
		if err != nil {
			return false, err
		} else if value != nil {
			// Do not have to backtrack :-)
			fmt.Println("do not have to backtrack! :-)")
			j++
		} else if j == 0 {
			fmt.Println("failed to resolve")
			b, _ := json.MarshalIndent(sortedAssignments.Assignments[j], "", "  ")
			fmt.Println(string(b))
			return false, nil
		} else {
			// Have to backtrack :-(
			fmt.Println("have to backtrack! :-(")
			delete(set, id)
			j--
		}
	}
	return true, nil
}

func checkBlankConstraint(node ld.Node, index int, set map[string]*Assignment, as AssignmentStack) (bool, *ld.BlankNode, error) {
	if blank, isBlank := node.(*ld.BlankNode); isBlank {
		if i, has := as.deps[blank.Attribute]; has {
			_, solved := set[blank.Attribute]
			return (i < index) || solved, blank, nil
		}
		return false, nil, errors.New("could not find blank node in dep map")
	}
	return true, nil, nil
}

func solveAssignment(set map[string]*Assignment, id string, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) ([]byte, error) {
	assignment, hasAssignment := set[id]
	index, hasIndex := as.deps[id]
	if assignment.References == nil || len(assignment.References) == 0 {
		return nil, errors.New("assignment.References is empty")
	} else if !hasAssignment || !hasIndex {
		return nil, errors.New("assignment or index not found in map")
	}

	reference := assignment.References[0]
	pivot := getValueKeyPrefix(reference, dataset, as)

	references := assignment.References[1:]
	constraints := []*Reference{}

	if assignment.Constraints != nil && len(assignment.Constraints) > 0 {
		counter := make([]byte, 8)
		for _, constraint := range assignment.Constraints {
			if m, M, err := checkBlankConstraint(constraint.M, index, set, as); err != nil {
				return nil, err
			} else if m {
				if n, N, err := checkBlankConstraint(constraint.N, index, set, as); err != nil {
					return nil, err
				} else if n {
					if (M != nil && M.Attribute == id) || (N != nil && N.Attribute == id) {
						constraints = append(constraints, constraint)
					} else {
						count, err := countReference(constraint, true, counter, txn, dataset, as)
						if err == badger.ErrKeyNotFound {
							return nil, nil
						} else if err != nil {
							return nil, err
						}
						constraint.Count = count
						references = append(references, constraint)
					}
				}
			}
		}
	}

	sort.Sort(SortedReferences(references))

	joins := make([][]byte, len(references))
	for i, ref := range references {
		joins[i] = getValueKeyPrefix(ref, dataset, as)
	}

	var value, iterator []byte
	var sources [][]byte
	var err error
	for {
		value, iterator, sources, err = join(pivot, assignment.Iterator, int(reference.Count), joins, txn)
		if err != nil {
			return nil, err
		}
		assignment.Value = value
		assignment.Iterator = iterator
		assignment.Sources = sources
		constraintSources, err := checkConstraints(constraints, as, dataset, txn)
		if err != nil {
			return nil, err
		} else if constraintSources != nil {

		}
	}

	assignment.Value = value
	assignment.Iterator = iterator
	assignment.Sources = sources

	return value, nil
}

func checkConstraints(constraints []*Reference, as AssignmentStack, dataset *ld.RDFDataset, txn *badger.Txn) ([]byte, error) {
	// for _, constraint := range constraints {

	// }
	return nil, nil
}
