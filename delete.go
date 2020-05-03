package styx

import (
	badger "github.com/dgraph-io/badger/v2"
	rdf "github.com/underlay/go-rdfjs"
)

// Delete a dataset from the database
func (s *Store) Delete(node rdf.Term) (err error) {
	dictionary := s.Config.Dictionary.Open(false)
	txn := s.Badger.NewTransaction(true)
	defer func() { txn.Discard(); dictionary.Commit() }()

	origin, err := dictionary.GetID(node, rdf.Default)
	if err != nil {
		return
	}

	quads, err := s.Config.QuadStore.Get(origin)
	if err != nil {
		return
	}

	txn, err = deleteQuads(origin, quads, dictionary, txn, s.Badger)
	if err != nil {
		return
	}

	err = txn.Commit()
	if err != nil {
		return
	}

	return s.Config.QuadStore.Delete(origin)
}

// Delete removes a dataset from the database
func deleteQuads(origin ID, quads [][4]ID, dictionary Dictionary, t *badger.Txn, db *badger.DB) (txn *badger.Txn, err error) {
	txn = t

	bc := newBinaryCache()
	uc := newUnaryCache()

	for _, quad := range quads {
		terms := [3]ID{quad[0], quad[1], quad[2]}
		var item *badger.Item
		p := TernaryPrefixes[0]
		key := assembleKey(p, false, quad[:3]...)
		item, err = txn.Get(key)
		if err == badger.ErrKeyNotFound {
			// This might happen because we filter everything from the given
			// origin, and there could be duplicate triples in a dataset.
			// No action needed.
			continue
		} else if err != nil {
			return
		}
		var statements []*Statement
		err = item.Value(func(val []byte) error {
			statements, err = getStatements(val)
			return err
		})
		if err != nil {
			return
		}
		val := make([]byte, 0)
		for _, x := range statements {
			if ID(x.base) != origin {
				val = append(val, x.String()...)
			}
		}
		if len(val) > 0 {
			txn, err = setSafe(key, val, txn, db)
			if err != nil {
				return
			}
		} else {
			err = bc.Decrement(0, terms[0], terms[1], uc, txn)
			if err != nil {
				return
			}

			err = bc.Decrement(3, terms[0], terms[2], uc, txn)
			if err != nil {
				return
			}

			txn, err = deleteSafe(key, txn, db)
			if err != nil {
				return
			}
			for p := Permutation(1); p < 3; p++ {
				a, b, c := major.permute(p, terms)

				err = bc.Decrement(p, terms[p], terms[(p+1)%3], uc, txn)
				if err != nil {
					return
				}

				err = bc.Decrement(p+3, terms[p], terms[(p+2)%3], uc, txn)
				if err != nil {
					return
				}

				key := assembleKey(TernaryPrefixes[p], false, a, b, c)
				txn, err = deleteSafe(key, txn, db)
				if err == badger.ErrKeyNotFound {
					// ???
					// This is more concerning...
				} else if err != nil {
					return
				}
			}
		}
	}

	txn, err = bc.Commit(db, txn)
	if err != nil {
		return
	}

	txn, err = uc.Commit(db, txn)
	if err != nil {
		return
	}

	return
}
