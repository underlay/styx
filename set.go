package styx

import (
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// SetJSONLD sets a JSON-LD document
func (db *Store) SetJSONLD(uri string, input interface{}, canonize bool) error {
	opts := ld.NewJsonLdOptions(uri)
	dataset, err := getDataset(input, opts)
	if err != nil {
		return err
	}
	return db.SetDataset(uri, dataset, canonize)
}

// SetDataset sets a piprate/json-gold dataset struct
func (db *Store) SetDataset(uri string, dataset *ld.RDFDataset, canonize bool) error {
	var quads []*ld.Quad
	if canonize {
		na := ld.NewNormalisationAlgorithm(Algorithm)
		na.Normalize(dataset)
		quads = na.Quads()
	} else {
		quads = make([]*ld.Quad, 0)
		for _, graph := range dataset.Graphs {
			quads = append(quads, graph...)
		}
	}
	return db.Set(uri, quads)
}

// Set is the entrypoint to inserting stuff
func (db *Store) Set(uri string, dataset []*ld.Quad) (err error) {
	if uri != "" && !(strings.Index(uri, "#") == -1 && db.Options.TagScheme.Test(uri+"#")) {
		return ErrTagScheme
	}

	txn := db.Badger.NewTransaction(true)
	defer func() { txn.Discard() }()

	values := newValueCache()
	uc := newUnaryCache()
	bc := newBinaryCache()

	// Check to see if this key is already in the database
	datasetKey := make([]byte, len(uri)+1)
	datasetKey[0] = DatasetPrefix
	copy(datasetKey[1:], uri)
	var datasetItem *badger.Item
	datasetItem, err = txn.Get(datasetKey)
	if err == badger.ErrKeyNotFound {
	} else if err != nil {
		return
	} else if txn, err = db.delete(uri, datasetItem, values, txn); err != nil {
		return
	}

	var origin iri
	origin, txn, err = getIRI(uri, values, txn, db.Sequence, db.Badger)

	quads := make([]byte, 0)
	for i, quad := range dataset {
		source := &Statement{
			Origin: origin,
			Index:  uint64(i),
		}

		terms := [3]Term{}
		for j := Permutation(0); j < 4; j++ {
			node := getNode(quad, j)
			var t Value
			t, txn, err = nodeToValue(node, origin, values, db.Options.TagScheme, txn, db.Sequence, db.Badger)
			if err != nil {
				return
			}

			if j == 3 {
				source.Graph = t
			} else {
				terms[j] = t.Term()
			}

			quads = append(quads, t.Term()...)
			if j == 3 {
				quads = append(quads, '\n')
			} else {
				quads = append(quads, '\t')
			}
		}

		var item *badger.Item
		for permutation := Permutation(0); permutation < 3; permutation++ {
			a, b, c := major.permute(permutation, terms)
			key := assembleKey(TernaryPrefixes[permutation], false, a, b, c)
			item, err = txn.Get(key)
			var val []byte
			if err == badger.ErrKeyNotFound {
				// Since this is a new key we have to increment two binary keys.
				ab, ba := permutation, ((permutation+1)%3)+3
				err = bc.Increment(ab, a, b, uc, txn)
				if err != nil {
					return
				}
				err = bc.Increment(ba, b, a, uc, txn)
				if err != nil {
					return
				}
				if permutation == 0 {
					val = []byte(source.Marshal(values, txn))
				}
				txn, err = setSafe(key, val, txn, db.Badger)
				if err != nil {
					return
				}
			} else if err != nil {
				return
			} else if permutation == 0 {
				val, err = item.ValueCopy(nil)
				if err != nil {
					return
				}
				val = append(val, source.Marshal(values, txn)...)
				txn, err = setSafe(key, val, txn, db.Badger)
				if err != nil {
					return
				}
			}
		}
	}

	txn, err = setSafe(datasetKey, quads, txn, db.Badger)
	if err != nil {
		return
	}

	txn, err = bc.Commit(db.Badger, txn)
	if err != nil {
		return
	}

	txn, err = uc.Commit(db.Badger, txn)
	if err != nil {
		return
	}

	return txn.Commit()
}

// getNode just indexes the Permutation into the appropriate term of the quad
func getNode(quad *ld.Quad, place Permutation) (node ld.Node) {
	switch place {
	case 0:
		node = quad.Subject
	case 1:
		node = quad.Predicate
	case 2:
		node = quad.Object
	case 3:
		node = quad.Graph
	}
	return
}
