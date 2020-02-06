package db

import (
	"encoding/binary"

	badger "github.com/dgraph-io/badger/v2"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/underlay/json-gold/ld"

	types "github.com/underlay/styx/types"
)

// Delete removes a dataset from the database
func (db *DB) Delete(c cid.Cid, dataset []*ld.Quad) (err error) {
	txn := db.Badger.NewTransaction(true)
	defer func() { txn.Discard() }()

	var datasetBytes []byte
	if c != cid.Undef {
		datasetBytes = c.Bytes()
	}
	datasetKey := types.AssembleKey(types.DatasetPrefix, datasetBytes, nil, nil)
	item, err := txn.Get(datasetKey)
	if err == badger.ErrKeyNotFound {
		return nil
	} else if err != nil {
		return err
	}

	err = txn.Delete(datasetKey)
	if err != nil {
		return err
	}

	ds := &types.Dataset{}
	err = item.Value(func(val []byte) error { return proto.Unmarshal(val, ds) })
	if err != nil {
		return err
	}

	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, ds.Id)
	valueKey := types.AssembleKey(types.ValuePrefix, value, nil, nil)
	err = txn.Delete(valueKey)
	if err != nil {
		return err
	}

	_, txn, err = types.Decrement(types.DatasetCountKey, 1, txn, db.Badger)
	if err != nil {
		return err
	}

	_, txn, err = types.Decrement(types.TripleCountKey, uint64(len(dataset)), txn, db.Badger)
	if err != nil {
		return err
	}

	indexCache := types.NewIndexCache()
	for i, quad := range dataset {
		s := types.NodeToTerm(quad.Subject, c, db.uri)
		p := types.NodeToTerm(quad.Predicate, c, db.uri)
		o := types.NodeToTerm(quad.Object, c, db.uri)

		S, err := indexCache.Get(s, txn)
		if err != nil {
			return err
		}

		P, err := indexCache.Get(p, txn)
		if err != nil {
			return err
		}

		O, err := indexCache.Get(o, txn)
		if err != nil {
			return err
		}

		S.Decrement(types.S)
		P.Decrement(types.P)
		O.Decrement(types.O)

		ids := [3][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 8)}
		binary.BigEndian.PutUint64(ids[0], S.Id)
		binary.BigEndian.PutUint64(ids[1], P.Id)
		binary.BigEndian.PutUint64(ids[2], O.Id)

		tripleKey := types.AssembleKey(types.TriplePrefixes[0], ids[0], ids[1], ids[2])
		tripleItem, err := txn.Get(tripleKey)
		if err != nil {
			return err
		}

		sourceList := &types.SourceList{}
		err = tripleItem.Value(func(val []byte) error { return proto.Unmarshal(val, sourceList) })
		if err != nil {
			return err
		}

		n, sources := 0, sourceList.GetSources()
		for _, statement := range sources {
			if statement.Origin == ds.Id && int(statement.Index) == i {
				continue
			} else {
				sources[n] = statement
				n++
			}
		}

		if n == 0 {
			// Delete all the triple keys
			for permutation := types.Permutation(0); permutation < 3; permutation++ {
				a, b, c := types.Major.Permute(permutation, ids)
				key := types.AssembleKey(types.TriplePrefixes[permutation], a, b, c)
				err = txn.Delete(key)
				if err != nil {
					return err
				}
			}
		} else {
			sourceList.Sources = sources[:n]
			val, err := proto.Marshal(sourceList)
			if err != nil {
				return err
			}
			err = txn.Set(tripleKey, val)
			if err != nil {
				return err
			}
		}

		for permutation := types.Permutation(0); permutation < 3; permutation++ {
			majorA, majorB, _ := types.Major.Permute(permutation, ids)
			majorKey := types.AssembleKey(types.MajorPrefixes[permutation], majorA, majorB, nil)
			_, txn, err = types.Decrement(majorKey, 1, txn, db.Badger)
			if err != nil {
				return err
			}

			minorA, minorB, _ := types.Minor.Permute(permutation, ids)
			minorKey := types.AssembleKey(types.MinorPrefixes[permutation], minorA, minorB, nil)
			_, txn, err = types.Decrement(minorKey, 1, txn, db.Badger)
			if err != nil {
				return err
			}
		}
	}

	txn, err = indexCache.Commit(db.Badger, txn)
	if err != nil {
		return err
	}

	return txn.Commit()
}
