package styx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strings"

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
value   3  p | a          Value       {p}
index   3  p | element    Index       {q}
----------------------------------------------

The delimiter (shown as a pipe above) is encoded as a tab := byte('\t')

When inserting a triple <|S P O|>, we perform 12-15 operations 😬
- We first look up each element's index key, if it exists.
  These are ['p', '\t', S...], ['p', '\t', P...], and ['p', '\t', O...].
  For each element, we either get a struct Index with a uint64 id, or we
  create a new one and write that to the index key. We also increment
  (or set to an initial 1) the Index.<position> counter: this is a count
  of the total number of times this id occurs in this position (.subject,
  .predicate, or .object) that we use a heuristic during query planning.
- We then insert the three triple keys. These are the rotations of the
	triple [a|s|p|o], [b|p|o|s], and [c|o|s|p], where s, p, and o are the
	uint64 ids we got from the index keys. The values for each of these keys
- Next we insert the three clockwise ("major")
*/

// TriplePrefixes address the value indices
var TriplePrefixes = [3]byte{'a', 'b', 'c'}
var triplePrefixMap = map[byte]uint8{'a': 0, 'b': 1, 'c': 2}

// MajorPrefixes address the "counter-clockwise" indices {spo, pos, osp}
var MajorPrefixes = [3]byte{'i', 'j', 'k'}
var majorPrefixMap = map[byte]uint8{'i': 0, 'j': 1, 'k': 2}

// MinorPrefixes address the "clockwise" indices {sop, pso, ops}
var MinorPrefixes = [3]byte{'x', 'y', 'z'}
var minorPrefixMap = map[byte]uint8{'x': 0, 'y': 1, 'z': 2}

// ValuePrefix keys translate uint64 ids to ld.Node values
var ValuePrefix = byte('p')

// IndexPrefix keys translate ld.Node values to uint64 ids
var IndexPrefix = byte('q')

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

func updateIndex(major bool, s, p, o []byte, txn *badger.Txn) ([3]uint64, error) {
	var countValues [3]uint64
	for i := byte(0); i < 3; i++ {
		var a, b, c []byte
		var prefix byte
		if major {
			a, b, c = permuteMajor(i, s, p, o)
			prefix = MajorPrefixes[i]
		} else {
			a, b, c = permuteMinor(i, s, p, o)
			prefix = MinorPrefixes[i]
		}
		// assembleKey will actually disregard c in this call
		key := assembleKey(prefix, a, b, c)
		if len(b) > 255 {
			return countValues, errors.New("Cannot insert a key longer than 255 characters")
		}
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
		err = txn.SetWithMeta(key, count, byte(len(b)))
		if err != nil {
			return countValues, err
		}
	}
	return countValues, nil
}

// This does all twelve db operations! :-)
func insert(origin string, dataset *ld.RDFDataset, txn *badger.Txn) error {
	// re-use the counter slice throughout iteration; yah?
	initialCount := make([]byte, 8)
	binary.BigEndian.PutUint64(initialCount, InitialCounter)

	c, err := cid.Decode(origin)
	if err != nil {
		return err
	}

	for _, graph := range dataset.Graphs {
		for index, quad := range graph {
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

			// Update the major index
			majorValues, err := updateIndex(true, s, p, o, txn)
			if err != nil {
				return err
			}

			// Update the minor index
			minorValues, err := updateIndex(false, s, p, o, txn)
			if err != nil {
				return err
			}

			// Sanity check that majors and minors have the same values
			for i := 0; i < 3; i++ {
				if majorValues[i] != minorValues[(i+1)%3] {
					return fmt.Errorf("Mismatching major & minor index values: %v %v", majorValues, minorValues)
				}
			}

			// Value & index loop
			for i := byte(0); i < 3; i++ {
				a, b, c := permuteMajor(i, s, p, o)
				valuePrefix := TriplePrefixes[i]
				// This is the value key.
				// assembleKey knows to pack all of a, b, and c because of the prefix.
				valueKey := assembleKey(valuePrefix, a, b, c)
				valueItem, err := txn.Get(valueKey)
				valueMeta := byte(len(c))
				if err == badger.ErrKeyNotFound {
					// Create a new SourceList container and write our source to it.
					sourceList := &SourceList{
						Sources: []*Source{source},
					}
					bytes, err := proto.Marshal(sourceList)
					if err != nil {
						return err
					}
					err = txn.SetWithMeta(valueKey, bytes, valueMeta)
					if err != nil {
						return err
					}
				} else if err != nil {
					return err
				} else {
					bytes, err := valueItem.ValueCopy(nil)
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
					err = txn.SetWithMeta(valueKey, bytes, valueMeta)
					if err != nil {
						return err
					}
				}

				// Index key
				indexPrefix := IndexPrefixes[i]
				indexKey := assembleKey(indexPrefix, a, b, c)
				indexItem, err := txn.Get(indexKey)
				indexMeta := byte(len(a))
				if err == badger.ErrKeyNotFound {
					err = txn.SetWithMeta(indexKey, initialCount, indexMeta)
					if err != nil {
						return err
					}
				} else if err != nil {
					return err
				} else {
					count := make([]byte, 8)
					count, err = indexItem.ValueCopy(count)
					if err != nil {
						return err
					}
					value := binary.BigEndian.Uint64(count) + 1
					binary.BigEndian.PutUint64(count, value)
					err = txn.SetWithMeta(indexKey, count, indexMeta)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func marshalNode(origin string, node ld.Node) []byte {
	if iri, isIRI := node.(*ld.IRI); isIRI {
		return []byte("<" + escape(iri.Value) + ">")
	} else if blank, isBlank := node.(*ld.BlankNode); isBlank {
		iri := fmt.Sprintf("<dweb:/ipfs/%s#%s>", origin, blank.Attribute)
		return []byte(iri)
	} else if literal, isLiteral := node.(*ld.Literal); isLiteral {
		escaped := escape(literal.GetValue())
		value := "\"" + escaped + "\""
		if literal.Datatype == ld.RDFLangString {
			value += "@" + literal.Language
		} else if literal.Datatype != ld.XSDString {
			value += "^^<" + escape(literal.Datatype) + ">"
		}
		return []byte(value)
	}
	return nil
}

// assembleKey will look at the prefix byte to determine
// how many of the elements {abc} to pack into the key.
// That means even if some of {abc} are nil, they'll still
// be "included" (and tab delimiters packed around them)
// if the prefix is one that calls for it.
func assembleKey(prefix byte, a, b, c []byte) []byte {
	keySize := 2 + len(a)
	if _, has := majorPrefixMap[prefix]; has {
		keySize += 1 + len(b)
	} else if _, has := minorPrefixMap[prefix]; has {
		keySize += 1 + len(b)
	} else if _, has := triplePrefixMap[prefix]; has {
		keySize += 1 + len(b) + 1 + len(c)
	}
	key := make([]byte, 2, keySize)
	key[0], key[1] = prefix, tab
	key = append(key, a...)
	if _, has := indexPrefixMap[prefix]; !has {
		key = append(key, tab)
		key = append(key, b...)
		if _, has := triplePrefixMap[prefix]; has {
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

func unescape(str string) string {
	str = strings.Replace(str, "\\\\", "\\", -1)
	str = strings.Replace(str, "\\\"", "\"", -1)
	str = strings.Replace(str, "\\n", "\n", -1)
	str = strings.Replace(str, "\\r", "\r", -1)
	str = strings.Replace(str, "\\t", "\t", -1)
	return str
}

func escape(str string) string {
	str = strings.Replace(str, "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "\n", "\\n", -1)
	str = strings.Replace(str, "\r", "\\r", -1)
	str = strings.Replace(str, "\t", "\\t", -1)
	return str
}
