package main

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"
)

/*
In this table, the 'p' that each key starts with is a single byte "prefix"
from the "prefixes" set. The prefix encodes both the key's type and rotation.

name    #  format         value type  prefixes
----------------------------------------------
triple  3  p | a | b | c  SourceList  {a b c}
major   3  p | a | b      uint64      {i j k}
minor   3  p | a | b      uint64      {x y z}
value   1  p | a          Value       {p}
index   1  p | element    Index       {q}
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

func insertValue(
	origin *cid.Cid,
	node ld.Node,
	position uint8,
	counter uint64,
	indexMap IndexMap,
	valueMap ValueMap,
	txn *badger.Txn,
) (uint64, error) {
	// The indexMap holds all of the (modified) Index structs that
	// we need to write back to the db at the end of insertion, and
	// valueMap holds all of the Value structs that we've *created*
	// (and incremented the counter for). Returns the uint64 id
	// (newly created or otherwise) for the node.

	value := marshalNode(origin, node)
	if index, has := indexMap[value]; has {
		index.Increment(position)
		return index.GetId(), nil
	}

	indexKey := make([]byte, 1, len(value)+1)
	indexKey[0] = IndexPrefix
	indexKey = append(indexKey, []byte(value)...)
	indexItem, err := txn.Get(indexKey)
	if err == badger.ErrKeyNotFound {
		// The node does not exist in the database; we have to
		// Create and write both keys
		id := counter + uint64(len(valueMap))
		index := &Index{Id: id}
		index.Increment(position)
		indexMap[value] = index
		valueMap[id] = nodeToValue(node, origin)
		return id, nil
	} else if err != nil {
		bytes, err := indexItem.ValueCopy(nil)
		if err != nil {
			return 0, err
		}
		index := &Index{}
		err = proto.Unmarshal(bytes, index)
		if err != nil {
			return 0, err
		}
		indexMap[value] = index
		index.Increment(position)
		return index.GetId(), nil
	} else {
		return 0, err
	}
}

func insertCount(major bool, s, p, o uint64, txn *badger.Txn) ([3]uint64, error) {
	var countValues [3]uint64
	for i := byte(0); i < 3; i++ {
		var a, b uint64
		var prefix byte
		if major {
			a, b, _ = permuteMajor(i, s, p, o)
			prefix = MajorPrefixes[i]
		} else {
			a, b, _ = permuteMinor(i, s, p, o)
			prefix = MinorPrefixes[i]
		}
		key := assembleKey(prefix, a, b, 0)
		item, err := txn.Get(key)
		count := make([]byte, 8)
		if err == badger.ErrKeyNotFound {
			countValues[i] = InitialCounter
		} else if err != nil {
			return countValues, err
		} else if count, err = item.ValueCopy(count); err != nil {
			return countValues, err
		} else {
			countValues[i] = binary.BigEndian.Uint64(count) + 1
		}

		binary.BigEndian.PutUint64(count, countValues[i])
		err = txn.SetWithMeta(key, count, prefix)
		if err != nil {
			return countValues, err
		}
	}
	return countValues, nil
}

// This does all sixteen db operations! :-)
// For now we only operate on the @default graph of the dataset
func insert(hash string, dataset *ld.RDFDataset, txn *badger.Txn) error {
	// re-use the counter slice throughout iteration; yah?
	initialCount := make([]byte, 8)
	binary.BigEndian.PutUint64(initialCount, InitialCounter)

	c, err := cid.Decode(hash)
	if err != nil {
		return err
	}

	origin := &c

	valueMap := ValueMap{}
	indexMap := IndexMap{}

	var counter uint64
	if counterItem, err := txn.Get(CounterKey); err == badger.ErrKeyNotFound {
		// No counter yet! Let's make one.
		counter = InitialCounter
		err = txn.Set(CounterKey, initialCount)
		if err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		counterBytes, err := counterItem.ValueCopy(nil)
		if err != nil {
			return err
		}
		counter = binary.BigEndian.Uint64(counterBytes)
	}

	for index, quad := range dataset.Graphs[DefaultGraph] {
		source := &Source{
			Cid:   c.Bytes(),
			Index: int32(index),
		}

		if quad.Graph != nil {
			source.Graph = quad.Graph.GetValue()
		}

		// Translate into uint64 ids
		s, err := insertValue(origin, quad.Subject, 0, counter, indexMap, valueMap, txn)
		if err != nil {
			return err
		}

		p, err := insertValue(origin, quad.Predicate, 1, counter, indexMap, valueMap, txn)
		if err != nil {
			return err
		}

		o, err := insertValue(origin, quad.Object, 2, counter, indexMap, valueMap, txn)
		if err != nil {
			return err
		}

		// Update the major index
		majorValues, err := insertCount(true, s, p, o, txn)
		if err != nil {
			return err
		}

		// Update the minor index
		minorValues, err := insertCount(false, s, p, o, txn)
		if err != nil {
			return err
		}

		// Sanity check that majors and minors have the same values
		for i := 0; i < 3; i++ {
			j := (i + 1) % 3
			if majorValues[i] != minorValues[j] {
				return fmt.Errorf("Mismatching major & minor index values: %v %v", majorValues, minorValues)
			}
		}

		// Triple loop
		for i := byte(0); i < 3; i++ {
			a, b, c := permuteMajor(i, s, p, o)
			triplePrefix := TriplePrefixes[i]

			// This is the value key.
			// assembleKey knows to pack all of a, b, and c because of the prefix.
			tripleKey := assembleKey(triplePrefix, a, b, c)
			tripleItem, err := txn.Get(tripleKey)
			if err == badger.ErrKeyNotFound {
				// Create a new SourceList container and write our source to it.
				sourceList := &SourceList{Sources: []*Source{source}}
				bytes, err := proto.Marshal(sourceList)
				if err != nil {
					return err
				}

				err = txn.SetWithMeta(tripleKey, bytes, triplePrefix)
				if err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else if tripleItem.UserMeta() != triplePrefix {
				return fmt.Errorf("Meta byte does not match the key prefix: %d %d", tripleItem.UserMeta(), triplePrefix)
			} else {
				bytes, err := tripleItem.ValueCopy(nil)
				if err != nil {
					return err
				}

				sourceList := &SourceList{}
				err = proto.Unmarshal(bytes, sourceList)
				if err != nil {
					return err
				}

				sources := sourceList.GetSources()
				sourceList.Sources = append(sources, source)
				bytes, err = proto.Marshal(sourceList)
				if err != nil {
					return err
				}

				err = txn.SetWithMeta(tripleKey, bytes, triplePrefix)
				if err != nil {
					return err
				}
			}
		}
	}

	// Write back the index keys we incremented
	for value, index := range indexMap {
		key := make([]byte, 1, len(value)+1)
		key[0] = IndexPrefix
		key = append(key, []byte(value)...)
		val, err := proto.Marshal(index)
		if err != nil {
			return err
		}
		err = txn.SetWithMeta(key, val, IndexPrefix)
		if err != nil {
			return err
		}
	}

	// Write any value keys we created
	for id, value := range valueMap {
		val, err := proto.Marshal(value)
		if err != nil {
			return err
		}

		key := make([]byte, 9)
		key[0] = ValuePrefix
		binary.BigEndian.PutUint64(key[1:], id)
		err = txn.SetWithMeta(key, val, ValuePrefix)
		if err != nil {
			return err
		}
	}

	// Counter was incremented iff values is not empty
	if len(valueMap) > 0 {
		counterVal := make([]byte, 8)
		newCounter := counter + uint64(len(valueMap))
		binary.BigEndian.PutUint64(counterVal, newCounter)
		err = txn.Set(CounterKey, counterVal)
		if err != nil {
			return err
		}
	}

	return nil
}
