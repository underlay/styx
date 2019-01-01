package styx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"
)

/*
#  format         value type  prefixes
--------------------------------------
3  p a \t b \t c  SourceList  {a b c}
6  p a \n b       uint64      {i j k l m n}
3  p a            uint64      {x y z}
----------------------------
14
*/

// ValuePrefixes address the value indices
var ValuePrefixes = [3]byte{'a', 'b', 'c'}
var valuePrefixMap = map[byte]uint8{'a': 0, 'b': 1, 'c': 2}

// MajorPrefixes address the "counter-clockwise" indices {spo, pos, osp}
var MajorPrefixes = [3]byte{'i', 'j', 'k'}
var majorPrefixMap = map[byte]uint8{'i': 0, 'j': 1, 'k': 2}

// MinorPrefixes address the "clockwise" indices {ops, sop, pso}
var MinorPrefixes = [3]byte{'l', 'm', 'n'}
var minorPrefixMap = map[byte]uint8{'l': 0, 'm': 1, 'n': 2}

// IndexPrefixes addres the single-element total counts {s, p, o}
var IndexPrefixes = [3]byte{'x', 'y', 'z'}
var indexPrefixMap = map[byte]uint8{'x': 0, 'y': 1, 'z': 2}

var keySizes = map[byte]int{
	'a': 3, 'b': 3, 'c': 3,
	'i': 2, 'j': 2, 'k': 2,
	'l': 2, 'm': 2, 'n': 2,
	'x': 1, 'y': 1, 'z': 1,
}

// Our delimiter of choice
// The "tab" is such a cute concept; idk why she's not more popular
const tab = byte('\t')
const newline = byte('\n')

func updateIndex(major bool, count, s, p, o []byte, txn *badger.Txn) ([3]uint64, error) {
	var countValues [3]uint64
	var permutation uint8
	for permutation = 0; permutation < 3; permutation++ {
		var a, b, c []byte
		var prefix byte
		if major {
			a, b, c = permuteMajor(permutation, s, p, o)
			prefix = MajorPrefixes[permutation]
		} else {
			a, b, c = permuteMinor(permutation, s, p, o)
			prefix = MinorPrefixes[permutation]
		}
		key := assembleKey(prefix, a, b, c)
		item, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			countValues[permutation] = InitialCounter
		} else if err != nil {
			return countValues, err
		} else if item.UserMeta() != prefix {
			return countValues, errors.New("Mismatching meta tag in major index")
		} else if count, err = item.ValueCopy(count); err != nil {
			countValues[permutation] = binary.BigEndian.Uint64(count) + 1
		}
		binary.BigEndian.PutUint64(count, countValues[permutation])
		err = txn.SetWithMeta(key, count, prefix)
		if err != nil {
			return countValues, err
		}
	}
	return countValues, nil
}

func marshalNode(origin string, node ld.Node) []byte {
	if blank, isBlank := node.(*ld.BlankNode); isBlank {
		return []byte(fmt.Sprintf("<ipfs:/dweb/%s#%s>", origin, blank.Attribute))
	}
	return []byte(node.GetValue())
}

func marshalQuad(quad *ld.Quad, origin string, index int) ([]byte, []byte, []byte, *Source, error) {
	c, err := cid.Decode(origin)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	source := &Source{
		Cid:   c.Bytes(),
		Index: int32(index),
	}
	if quad.Graph != nil {
		source.Graph = quad.Graph.GetValue()
	}

	s := marshalNode(origin, quad.Subject)
	p := marshalNode(origin, quad.Predicate)
	o := marshalNode(origin, quad.Object)

	return s, p, o, source, nil
}

// This does all twelve db operations! :-)
func insert(origin string, dataset *ld.RDFDataset, txn *badger.Txn) error {
	// re-use the counter slice throughout iteration; yah?
	count := make([]byte, 8)
	var permutation uint8
	for _, graph := range dataset.Graphs {
		for index, quad := range graph {
			s, p, o, source, err := marshalQuad(quad, origin, index)
			if err != nil {
				return err
			}

			// Update the major index
			majorValues, err := updateIndex(true, count, s, p, o, txn)
			if err != nil {
				return err
			}

			// Update the minor index
			minorValues, err := updateIndex(false, count, s, p, o, txn)
			if err != nil {
				return err
			}

			// Sanity check that majors and minors have the same values
			for permutation = 0; permutation < 3; permutation++ {
				if majorValues[permutation] != minorValues[permutation] {
					return fmt.Errorf("Mismatching major & minor index values: %v %v", majorValues, minorValues)
				}
			}

			// Value & index loop
			for permutation = 0; permutation < 3; permutation++ {
				a, b, c := permuteMajor(permutation, s, p, o)
				prefix := ValuePrefixes[permutation]
				// This is the value key.
				// assembleKey knows to pack all of a, b, and c because of the prefix.
				key := assembleKey(prefix, a, b, c)
				item, err := txn.Get(key)
				if err == badger.ErrKeyNotFound {
					// Create a new SourceList container and write our source to it.
					sourceList := &SourceList{
						Sources: []*Source{source},
					}
					bytes, err := proto.Marshal(sourceList)
					if err != nil {
						return err
					}
					err = txn.SetWithMeta(key, bytes, prefix)
					if err != nil {
						return err
					}
				} else if err != nil {
					return err
				} else {
					bytes, err := item.ValueCopy(nil)
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
					err = txn.SetWithMeta(key, bytes, prefix)
					if err != nil {
						return err
					}
				}

				// Index key
				indexPrefix := IndexPrefixes[permutation]
				indexKey := assembleKey(indexPrefix, a, b, c)
				indexItem, err := txn.Get(indexKey)
				if err == badger.ErrKeyNotFound {
					binary.BigEndian.PutUint64(count, InitialCounter)
					err = txn.SetWithMeta(key, count, indexPrefix)
					if err != nil {
						return err
					}
				} else if err != nil {
					return err
				} else {
					count, err = indexItem.ValueCopy(count)
					if err != nil {
						return err
					}
					value := binary.BigEndian.Uint64(count) + 1
					binary.BigEndian.PutUint64(count, value)
					err = txn.SetWithMeta(key, count, indexPrefix)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func assembleKey(prefix byte, a, b, c []byte) []byte {
	keySize := 2 + len(a)
	if _, has := majorPrefixMap[prefix]; has {
		keySize += 1 + len(b)
	} else if _, has := minorPrefixMap[prefix]; has {
		keySize += 1 + len(b)
	} else if _, has := valuePrefixMap[prefix]; has {
		keySize += 1 + len(b) + 1 + len(c)
	}
	key := make([]byte, 2, keySize)
	key[0] = prefix
	key[1] = tab
	key = append(key, a...)
	if _, has := indexPrefixMap[prefix]; !has {
		key = append(key, tab)
		key = append(key, b...)
		if _, has := valuePrefixMap[prefix]; has {
			key = append(key, tab)
			key = append(key, c...)
		}
	}
	return key
}

func permuteMajor(permutation uint8, s, p, o []byte) ([]byte, []byte, []byte) {
	if permutation == 0 {
		return s, p, o
	} else if permutation == 1 {
		return p, o, s
	} else if permutation == 2 {
		return o, s, p
	}
	log.Fatalln("Invalid major permutation")
	return nil, nil, nil
}

func permuteMinor(permutation uint8, s, p, o []byte) ([]byte, []byte, []byte) {
	if permutation == 0 {
		return s, o, p
	} else if permutation == 1 {
		return p, s, o
	} else if permutation == 2 {
		return o, p, s
	}
	log.Fatalln("Invalid minor permutation")
	return nil, nil, nil
}
