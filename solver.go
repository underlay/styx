package styx

import (
	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

func getKey(id string, reference Reference, as AssignmentStack) string {
	var p, q string
	if reference.P != "" {
		p = as.maps[as.deps[reference.P]][reference.P].Value
	}
	if reference.Q != "" {
		q = as.maps[as.deps[reference.Q]][reference.Q].Value
	}
	if reference.Place == 1 {
		if reference.P == "" {

		}
	} else if reference.Place == 2 {

	} else if reference.Place == 3 {

	}
}

func solveAssignment(id string, assignment *Assignment, as AssignmentStack, txn *badger.Txn) {
	// TODO: have a better sorting heuristic than iteration
	for i, reference := range assignment.References {
		key := getKey(id, reference, as)
	}
}

func solveAssignmentMap(am AssignmentMap, as AssignmentStack, txn *badger.Txn) {
	// TODO: have a better sorting heuristic than iteration
	for id, assignment := range am {
		solveAssignment(id, assignment, as, txn)
	}
}

func query(dataset *ld.RDFDataset, db *badger.DB, cb func(AssignmentStack) error) error {
	as := getAssignmentStack(dataset)
	return db.View(func(txn *badger.Txn) error {
		for _, am := range as.maps {
			solveAssignmentMap(am, as, txn)
		}
		return cb(as)
	})
}
