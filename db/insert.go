package db

import (
	"encoding/binary"
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger/v2"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/underlay/json-gold/ld"

	types "github.com/underlay/styx/types"
)

// Insert is the entrypoint to inserting stuff
func (db *DB) Insert(c cid.Cid, dataset []*ld.Quad) (err error) {
	// First we canonize and serialize the dataset

	txn := db.Badger.NewTransaction(true)
	defer txn.Discard()

	var datasetBytes []byte
	if c != cid.Undef {
		datasetBytes = c.Bytes()
	}
	datasetKey := types.AssembleKey(types.DatasetPrefix, datasetBytes, nil, nil)

	// Check to see if this document is already in the database
	_, err = txn.Get(datasetKey)
	if err != badger.ErrKeyNotFound {
		if err == nil {
			log.Println("Dataset already inserted")
		}
		return
	}

	var origin uint64
	origin, err = db.Sequence.Next()
	if err != nil {
		return
	}

	values := types.NewValueCache()
	indices := types.NewIndexCache()

	values.Set(origin, types.Cid(c))

	var val []byte
	val, err = proto.Marshal(&types.Dataset{Id: origin})
	if err != nil {
		return
	}

	txn, err = types.SetSafe(datasetKey, val, txn, db.Badger)
	if err != nil {
		return
	}

	for index, quad := range dataset {
		var g string
		if quad.Graph != nil {
			g = quad.Graph.GetValue()
		}

		source := &types.Statement{
			Origin: origin,
			Index:  uint32(index),
			Graph:  g,
		}

		// Get the uint64 ids for the subject, predicate, and object
		ids := [3][]byte{}
		for permutation := types.Permutation(0); permutation < 3; permutation++ {
			ids[permutation], err = db.getID(c, origin, quad, permutation, indices, values, txn)
			if err != nil {
				return
			}
		}

		// Set counts
		var major, minor [3]uint64
		var key []byte
		for permutation := types.Permutation(0); permutation < 3; permutation++ {
			// Major Key
			majorA, majorB, _ := types.Major.Permute(permutation, ids)
			key = types.AssembleKey(types.MajorPrefixes[permutation], majorA, majorB, nil)
			major[permutation], txn, err = types.Increment(key, 1, txn, db.Badger)
			if err != nil {
				return
			}

			// Minor Key
			minorA, minorB, _ := types.Minor.Permute(permutation, ids)
			key = types.AssembleKey(types.MinorPrefixes[permutation], minorA, minorB, nil)
			minor[permutation], txn, err = types.Increment(key, 1, txn, db.Badger)
			if err != nil {
				return
			}
		}

		// Sanity check that majors and minors have the same values
		for i := 0; i < 3; i++ {
			j := (i + 1) % 3
			if major[i] != minor[j] {
				return fmt.Errorf(
					"Mismatching major & minor index values:\n  %v %v\n  <%s %s %s>",
					// "Mismatching major & minor index values:\n  %v %v\n  <%s %s %s>\n  <%02d %02d %02d>",
					major, minor,
					quad.Subject.GetValue(),
					quad.Predicate.GetValue(),
					quad.Object.GetValue(),
					// binary.BigEndian.Uint64(s),
					// binary.BigEndian.Uint64(p),
					// binary.BigEndian.Uint64(o),
				)
			}
		}

		// Triple loop
		var item *badger.Item
		for i := types.Permutation(0); i < 3; i++ {
			a, b, c := types.Major.Permute(i, ids)
			key := types.AssembleKey(types.TriplePrefixes[i], a, b, c)
			var val []byte
			item, err = txn.Get(key)
			if err == badger.ErrKeyNotFound {
				if i == 0 {
					// Create a new SourceList container with the source
					sources := &types.SourceList{Sources: []*types.Statement{source}}
					val, err = proto.Marshal(sources)
					if err != nil {
						return
					}
				}
				txn, err = types.SetSafe(key, val, txn, db.Badger)
				if err != nil {
					return
				}
			} else if err != nil {
				return
			} else if i == 0 {
				sources := &types.SourceList{}
				err = item.Value(func(val []byte) error { return proto.Unmarshal(val, sources) })
				if err != nil {
					return
				}
				sources.Sources = append(sources.GetSources(), source)
				val, err = proto.Marshal(sources)
				if err != nil {
					return
				}
				txn, err = types.SetSafe(key, val, txn, db.Badger)
				if err != nil {
					return
				}
			}
		}
	}

	_, txn, err = types.Increment(types.DatasetCountKey, 1, txn, db.Badger)
	if err != nil {
		return
	}

	_, txn, err = types.Increment(types.TripleCountKey, uint64(len(dataset)), txn, db.Badger)
	if err != nil {
		return
	}

	txn, err = indices.Commit(db.Badger, txn)
	if err != nil {
		return err
	}

	txn, err = values.Commit(db.Badger, txn)
	if err != nil {
		return err
	}

	return txn.Commit()
}

func (db *DB) getID(
	c cid.Cid, origin uint64,
	quad *ld.Quad, place types.Permutation,
	indices types.IndexCache,
	values types.ValueCache,
	txn *badger.Txn,
) ([]byte, error) {
	id := make([]byte, 8)
	node := types.GetNode(quad, place)
	term := types.NodeToTerm(node, c, db.uri)
	index, err := indices.Get(term, txn)
	if err == badger.ErrKeyNotFound {
		index = &types.Index{}
		index.Id, err = db.Sequence.Next()
		value := types.NodeToValue(node, origin, db.uri, txn)
		values.Set(index.Id, value)
		indices.Set(term, index)
	} else if err != nil {
		return nil, err
	}
	index.Increment(place)
	binary.BigEndian.PutUint64(id, index.Id)
	return id, nil
}
