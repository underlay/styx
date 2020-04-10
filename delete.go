package styx

import (
	badger "github.com/dgraph-io/badger/v2"
)

func (db *Styx) Delete(uri string) (err error) {
	txn := db.Badger.NewTransaction(true)
	defer func() { txn.Discard() }()
	values := newValueCache()
	key := make([]byte, len(uri)+1)
	key[0] = DatasetPrefix
	copy(key[1:], uri)
	var item *badger.Item
	item, err = txn.Get(key)
	if err != nil {
		return err
	}

	txn, err = db.delete(uri, item, values, txn)
	if err != nil {
		return
	}

	err = txn.Delete(key)
	if err != nil {
		return
	}

	return txn.Commit()
}

// Delete removes a dataset from the database
func (db *Styx) delete(uri string, item *badger.Item, values valueCache, t *badger.Txn) (txn *badger.Txn, err error) {
	txn = t
	quads, err := db.get(item)
	if err != nil {
		return
	}

	origin, err := values.GetID(uri, txn)
	if err != nil {
		return
	}

	bc := newBinaryCache()
	uc := newUnaryCache()

	for _, quad := range quads {
		terms := [3]Term{quad[0].Term(), quad[1].Term(), quad[2].Term()}
		var item *badger.Item
		p := TernaryPrefixes[0]
		key := assembleKey(p, false, terms[:]...)
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
			if x.Origin != origin {
				val = append(val, x.Marshal(values, txn)...)
			}
		}
		if len(val) > 0 {
			txn, err = setSafe(key, val, txn, db.Badger)
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

			txn, err = deleteSafe(key, txn, db.Badger)
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
				txn, err = deleteSafe(key, txn, db.Badger)
				if err == badger.ErrKeyNotFound {
					// ???
					// This is more concerning...
				} else if err != nil {
					return
				}
			}
		}
	}

	txn, err = bc.Commit(db.Badger, txn)
	if err != nil {
		return
	}

	txn, err = uc.Commit(db.Badger, txn)
	if err != nil {
		return
	}

	return
}
