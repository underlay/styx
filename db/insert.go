package db

import (
	"encoding/binary"
	"fmt"
	"log"
	"time"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

/*
In this table, the 'p' that each key starts with is a single byte "prefix"
from the "prefixes" set. The prefix encodes both the key's type and rotation.

name    #  key format     value type  prefixes
----------------------------------------------
triple  3  p | a | b | c  SourceList  {a b c}
major   3  p | a | b      uint64      {i j k}
minor   3  p | a | b      uint64      {x y z}
value   1  p | a          Value       {p}
index   1  p | element    Index       {q}
graph   1  p | cid        ISO Date    {g}
counter 0                 uint64      {>}
----------------------------------------------

When inserting a triple <|S P O|>, we perform 12-15 operations 😬
- We first look up each element's index key, if it exists.
  For each element, we either get a struct Index with a uint64 id, or we
  create a new one and write that to the index key. We also increment
  (or set to an initial 1) the Index.<position> counter: this is a count
	of the total number of times this id occurs in this position
	(.subject, .predicate, or .object) that we use a heuristic during
	query planning.
- We then insert the three triple keys. These are the rotations of the
	triple [a|s|p|o], [b|p|o|s], and [c|o|s|p], where s, p, and o are the
	uint64 ids we got from the index keys. The values for each of these
	keys are SourceList structs.
- Next we insert the three clockwise ("major") double keys with prefixes {ijk}
- Next we insert the three counter-clockwise ("minor") double keys with
	prefixes {xyz}
*/

func insert(cid cid.Cid, graph string, quads []*ld.Quad, txn *badger.Txn) (err error) {
	graphID := fmt.Sprintf("%s#%s", cid.String(), graph)
	graphKey := types.AssembleKey(types.GraphPrefix, []byte(graphID), nil, nil)

	var item *badger.Item

	// Check to see if this document is already in the database
	if item, err = txn.Get(graphKey); err != badger.ErrKeyNotFound {
		return item.Value(func(val []byte) (err error) {
			log.Printf("Duplicate document inserted previously on %s\n", string(val))
			return
		})
	}

	// Write the current date to the graph key
	date := []byte(time.Now().Format(time.RFC3339))
	if err = txn.Set(graphKey, date); err != nil {
		return
	}

	var root uint64
	value := make([]byte, 8)
	if item, err = txn.Get(types.CounterKey); err == badger.ErrKeyNotFound {
		root = types.InitialCounter
	} else if err != nil {
		return
	} else if value, err = item.ValueCopy(value); err != nil {
		return
	} else {
		root = binary.BigEndian.Uint64(value)
	}

	values := types.ValueMap{}
	indices := types.IndexMap{}

	for index, quad := range quads {
		source := &types.Source{
			Cid:   cid.Bytes(),
			Graph: graph,
			Index: int32(index),
		}

		if quad.Graph != nil {
			source.Graph = quad.Graph.GetValue()
		}

		// Get the uint64 ids for the subject, predicate, and object
		var s, p, o []byte
		if s, err = getID(cid, quad.Subject, 0, root, indices, values, txn); err != nil {
			return
		} else if p, err = getID(cid, quad.Predicate, 1, root, indices, values, txn); err != nil {
			return
		} else if o, err = getID(cid, quad.Object, 2, root, indices, values, txn); err != nil {
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
			sources := &types.SourceList{}
			var value []byte
			if item, err = txn.Get(key); err == badger.ErrKeyNotFound {
				// Create a new SourceList container with the source
				sources.Sources = []*types.Source{source}
				if value, err = proto.Marshal(sources); err != nil {
					return
				} else if err = txn.Set(key, value); err != nil {
					return
				}
			} else if err != nil {
				return
			} else if value, err = item.ValueCopy(nil); err != nil {
				return
			} else if err = proto.Unmarshal(value, sources); err != nil {
				return
			} else {
				sources.Sources = append(sources.GetSources(), source)
				if value, err = proto.Marshal(sources); err != nil {
					return
				} else if err = txn.Set(key, value); err != nil {
					return
				}
			}
		}
	}

	if err = indices.Commit(txn); err != nil {
		return
	} else if err = values.Commit(txn); err != nil {
		return
	}

	// Counter was incremented iff values is not empty
	if len(values) > 0 {
		next := root + uint64(len(values))
		binary.BigEndian.PutUint64(value, next)
		err = txn.Set(types.CounterKey, value)
	}

	return
}

func getID(
	origin cid.Cid,
	node ld.Node,
	place uint8,
	root uint64,
	indices types.IndexMap,
	values types.ValueMap,
	txn *badger.Txn,
) ([]byte, error) {
	ID := make([]byte, 8)
	value := types.NodeToValue(origin, node)
	v := value.GetValue()

	if index, has := indices[v]; has {
		index.Increment(place)
		binary.BigEndian.PutUint64(ID, index.GetId())
		return ID, nil
	}

	// Assemble the index key
	key := make([]byte, 1, len(v)+1)
	key[0] = types.IndexPrefix
	key = append(key, []byte(v)...)

	var index *types.Index
	if item, err := txn.Get(key); err == badger.ErrKeyNotFound {
		// Generate a new id and create an Index struct for it
		id := values.InsertNode(value, root)
		index = &types.Index{Id: id}
	} else if err != nil {
		return nil, err
	} else {
		// Unmarshal the value into an Index struct
		index = &types.Index{}
		if value, err := item.ValueCopy(nil); err != nil {
			return nil, err
		} else if err := proto.Unmarshal(value, index); err != nil {
			return nil, err
		}
	}

	indices[v] = index
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
	value := make([]byte, 8)

	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		count = types.InitialCounter
	} else if err != nil {
		return
	} else if value, err = item.ValueCopy(value); err != nil {
		return
	} else {
		count = binary.BigEndian.Uint64(value) + 1
	}

	binary.BigEndian.PutUint64(value, count)
	err = txn.Set(key, value)
	return
}
