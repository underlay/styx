package db

import (
	"encoding/binary"
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

func (db *DB) insert(c cid.Cid, quads []*ld.Quad, label string, graph []int, txn *badger.Txn) (err error) {
	value, err := proto.Marshal(&types.Blank{
		Cid: c.Bytes(),
		Id:  label,
	})

	if err != nil {
		return err
	}

	graphKey := types.AssembleKey(types.GraphPrefix, value, nil, nil)

	var item *badger.Item

	// Check to see if this document is already in the database
	if item, err = txn.Get(graphKey); err != badger.ErrKeyNotFound {
		return item.Value(func(val []byte) (err error) {
			log.Printf("Duplicate document inserted previously on %s\n", string(val))
			return
		})
	}

	// Write the current date to the graph key
	if err = txn.Set(graphKey, nil); err != nil {
		return
	}

	valueMap := types.ValueMap{}
	indexMap := types.IndexMap{}

	for index, quad := range quads {
		var g string
		if quad.Graph != nil {
			g = quad.Graph.GetValue()
		}

		if g != label {
			continue
		}

		source := &types.Source{
			Cid:   c.Bytes(),
			Index: uint32(index),
			Graph: g,
		}

		// Get the uint64 ids for the subject, predicate, and object
		var s, p, o []byte
		if s, err = db.getID(c, quad.Subject, 0, indexMap, valueMap, txn); err != nil {
			return
		} else if p, err = db.getID(c, quad.Predicate, 1, indexMap, valueMap, txn); err != nil {
			return
		} else if o, err = db.getID(c, quad.Object, 2, indexMap, valueMap, txn); err != nil {
			return
		}

		var major, minor [3]uint64
		if major, minor, err = setCounts(s, p, o, txn); err != nil {
			return
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
		for i := uint8(0); i < 3; i++ {
			a, b, c := permuteMajor(i, s, p, o)
			key := types.AssembleKey(types.TriplePrefixes[i], a, b, c)
			// sources := &types.SourceList{}
			var val []byte
			if item, err = txn.Get(key); err == badger.ErrKeyNotFound {
				if i == 0 {
					// Create a new SourceList container with the source
					sources := &types.SourceList{Sources: []*types.Source{source}}
					if val, err = proto.Marshal(sources); err != nil {
						return
					}
				}

				if err = txn.Set(key, val); err != nil {
					return
				}
			} else if err != nil {
				return
			} else if i == 0 {
				sources := &types.SourceList{}
				if val, err = item.ValueCopy(nil); err != nil {
					return
				} else if err = proto.Unmarshal(val, sources); err != nil {
					return
				}
				sources.Sources = append(sources.GetSources(), source)
				if val, err = proto.Marshal(sources); err != nil {
					return
				} else if err = txn.Set(key, val); err != nil {
					return
				}
			}
		}
	}

	if err = indexMap.Commit(txn); err != nil {
		return
	} else if err = valueMap.Commit(txn); err != nil {
		return
	}

	return
}

func (db *DB) getID(
	origin cid.Cid,
	node ld.Node,
	place uint8,
	indexMap types.IndexMap,
	valueMap types.ValueMap,
	txn *badger.Txn,
) ([]byte, error) {
	ID := make([]byte, 8)
	value := types.NodeToValue(origin, node)
	v := value.GetValue()

	if index, has := indexMap[v]; has {
		index.Increment(place)
		binary.BigEndian.PutUint64(ID, index.GetId())
		return ID, nil
	}

	// Assemble the index key
	key := make([]byte, 1, len(v)+1)
	key[0] = types.IndexPrefix
	key = append(key, []byte(v)...)

	// var index *types.Index
	index := &types.Index{}
	if item, err := txn.Get(key); err == badger.ErrKeyNotFound {
		// Generate a new id and create an Index struct for it
		if index.Id, err = db.Sequence.Next(); err != nil {
			return nil, err
		}
		valueMap[index.Id] = value
	} else if err != nil {
		return nil, err
	} else if val, err := item.ValueCopy(nil); err != nil {
		return nil, err
	} else if err := proto.Unmarshal(val, index); err != nil {
		return nil, err

	}

	indexMap[v] = index
	index.Increment(place)
	binary.BigEndian.PutUint64(ID, index.GetId())

	return ID, nil
}

func setCounts(s, p, o []byte, txn *badger.Txn) (major [3]uint64, minor [3]uint64, err error) {
	var key []byte
	for i := uint8(0); i < 3; i++ {
		// Major Key
		majorA, majorB, _ := permuteMajor(i, s, p, o)
		key = types.AssembleKey(types.MajorPrefixes[i], majorA, majorB, nil)
		if major[i], err = setCount(key, txn); err != nil {
			return
		}

		// Minor Key
		minorA, minorB, _ := permuteMinor(i, s, p, o)
		key = types.AssembleKey(types.MinorPrefixes[i], minorA, minorB, nil)
		if minor[i], err = setCount(key, txn); err != nil {
			return
		}
	}
	return
}

// setCount handles both major and minor keys, writing the initial counter
// for nonexistent keys and incrementing existing ones
func setCount(key []byte, txn *badger.Txn) (count uint64, err error) {
	val := make([]byte, 8)

	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		count = uint64(1)
	} else if err != nil {
		return
	} else if val, err = item.ValueCopy(val); err != nil {
		return
	} else {
		count = binary.BigEndian.Uint64(val) + 1
	}

	binary.BigEndian.PutUint64(val, count)
	err = txn.Set(key, val)
	return
}
