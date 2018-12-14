package styx

import (
	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

func query(dataset *ld.RDFDataset, db *badger.DB, cb func(AssignmentStack) error) error {
	as := getAssignmentStack(dataset)
	return db.View(func(txn *badger.Txn) error {
		for _, am := range as.maps {
			solveAssignmentMap(am, as, dataset, txn)
		}
		return cb(as)
	})
}
