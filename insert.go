package styx

import (
	"encoding/binary"
	"log"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

/*
Some term definitions:
name         type    description
--------------------------------
permutation  uint8   an integer {0, 1, 2} corresponding to a rotation of three elements
prefix       byte    a byte {a, b, c, i, j, k, x, y, z}
s, p, o      string  escaped subject, predicate, or object quad elements, respectively
m, n         string  escaped quad elements, but in the order they appear in the key of an entry
key          []byte  slice that exists (or will exist) as a real key in Badger
counter      uint64  value of an index key, or postfix of a value key

What actually are keys?
They begin with a prefix byte, then a tab. Then comes m, then a tab, then n.
If the key is a major or minor index key, then that's it.
But if the key is a value key, it continues with another tab, and then the counter bytes.
----- keys -----
<prefix> <tab> <m...> <tab> <n...> (<tab> <counter...>)?
----------------

An "index key" is a key for a major or minor index scheme.
Index keys start with {i j k x y z} and their values are big-endian uint64s.
These uint64s are "counters" whose value is the total number of quad entries
for that particular <m... n...> pair.

When we insert a quad, we increment all six counters for index keys
<s p>, <p o>, <o s>, <o p>, <s o>, and <p s>.

A "value key" is a key for a value index scheme.
Crucially, even though there are six index counters to increment, they occur in three
pairs of identical values (the <s p> entries are identical to the <p s> entries, etc),
so we only have three value keys to write (arbitrarily choosing the three "clockwise"
rotations <s p>, <p o>, and <o s>). Value keys start with {a b c}, end with a big-endian
uint64 (since Badger iterates in lexicographic order), and their values look like
---- values ----
<base-58 cid...> <tab> (<graph label> <tab>)? <decimal index> <newline> <value...>
----------------

Values for m & n, by prefix:
prefix  m  n
------------
     a  p  o
     b  o  s
		 c  s  p
		 i  p  o
		 j  o  s
		 k  s  p
		 x  o  p
		 y  s  o
		 z  p  s
*/

// ValuePrefixes address the value indices
var ValuePrefixes = [3]byte{'a', 'b', 'c'}
var valuePrefixMap = map[byte]uint8{'a': 0, 'b': 1, 'c': 2}

// MajorPrefixes address the "clockwise" indices {spo, pos, osp}
var MajorPrefixes = [3]byte{'i', 'j', 'k'}
var majorPrefixMap = map[byte]uint8{'i': 0, 'j': 1, 'k': 2}

// MinorPrefixes address the "counter-clockwise" indices {ops, sop, pso}
var MinorPrefixes = [3]byte{'x', 'y', 'z'}
var minorPrefixMap = map[byte]uint8{'x': 0, 'y': 1, 'z': 2}

// A PermuteIndex is either permuteMajor, permuteMinor, or permuteValue
type PermuteIndex func(uint8, string, string, string) (byte, string, string, string)

// Our delimiter of choice
// The "tab" is such a cute concept; idk why she's not more popular
const tab = byte('\t')

// updateIndex takes a PermuteIndex *function* so that we can re-use
// most of its body to update both major and minor indices.
func updateIndex(
	counter []byte,
	s string,
	p string,
	o string,
	permuteIndex PermuteIndex,
	txn *badger.Txn,
) ([3]uint64, error) {
	// So this is inside updateIndex, where we're updating *one specific index*.
	// By "update" we mean assemble the index key for the
	// All index-specific stuff is packaged inside the permuteIndex we're passed.
	// Our result is a three-tuple of incremented counter values.
	var counterValues [3]uint64
	var permutation uint8
	for permutation = 0; permutation < 3; permutation++ {
		// Here we don't actually care about the last value element,
		// only about the prefix and the two key elements.
		prefix, m, n, _ := permuteIndex(permutation, s, p, o)
		key := assembleKey(prefix, m, n, nil)
		index, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			counterValues[permutation] = InitialCounter
		} else if err != nil {
			return counterValues, err
		} else if index.UserMeta() != prefix {
			log.Fatalln("Mismatching meta tag in major index")
		} else {
			counter, err = index.ValueCopy(counter)
			if err != nil {
				return counterValues, err
			}
			counterValues[permutation] = binary.BigEndian.Uint64(counter)
		}

		binary.BigEndian.PutUint64(counter, counterValues[permutation]+1)
		err = txn.SetWithMeta(key, counter, prefix)
		if err != nil {
			return counterValues, err
		}
	}
	return counterValues, nil
}

func insert(cid string, dataset *ld.RDFDataset, txn *badger.Txn) error {
	// re-use the counter slice throughout iteration
	counter := make([]byte, 8)
	var permutation uint8
	for _, graph := range dataset.Graphs {
		for i, quad := range graph {
			s, p, o, g := marshallQuad(quad, cid, i)

			// Update the major index
			majorValues, err := updateIndex(counter, s, p, o, permuteMajor, txn)
			if err != nil {
				return err
			}

			// Update the minor index
			minorValues, err := updateIndex(counter, s, p, o, permuteMinor, txn)
			if err != nil {
				return err
			}

			// Sanity check that majors and minors have the same values
			for permutation = 0; permutation < 3; permutation++ {
				if majorValues[permutation] != minorValues[permutation] {
					log.Fatalln("Mismatching major & minor index values", majorValues, minorValues)
				}
			}

			for permutation = 0; permutation < 3; permutation++ {
				prefix, m, n, value := permuteValue(permutation, s, p, o)
				val := append(g, value...)
				binary.BigEndian.PutUint64(counter, majorValues[i]) // warning: opinion
				key := assembleKey(prefix, m, n, counter)
				err = txn.SetWithMeta(key, val, prefix)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func assembleKey(prefix byte, m string, n string, counter []byte) []byte {
	length := 1 + 1 + len(m) + 1 + len(n)
	if counter != nil {
		length += 1 + len(counter)
	}
	key := make([]byte, length)
	key = append(key, prefix)
	key = append(key, tab)
	key = append(key, m...)
	key = append(key, tab)
	key = append(key, n...)
	if counter != nil {
		key = append(key, tab)
		key = append(key, counter...)
	}
	return key
}

func permuteValue(permutation uint8, s string, p string, o string) (byte, string, string, string) {
	prefix := ValuePrefixes[permutation]
	if prefix == 'a' {
		return prefix, p, o, s
	} else if prefix == 'b' {
		return prefix, o, s, p
	} else if prefix == 'c' {
		return prefix, s, p, o
	}
	log.Fatalln("Invalid value permutation")
	return prefix, "", "", ""
}

func permuteMajor(permutation uint8, s string, p string, o string) (byte, string, string, string) {
	prefix := MajorPrefixes[permutation]
	if prefix == 'i' {
		return prefix, p, o, s
	} else if prefix == 'j' {
		return prefix, o, s, p
	} else if prefix == 'k' {
		return prefix, s, p, o
	}
	log.Fatalln("Invalid major permutation")
	return prefix, "", "", ""
}

func permuteMinor(permutation uint8, s string, p string, o string) (byte, string, string, string) {
	prefix := MinorPrefixes[permutation]
	if prefix == 'x' {
		return prefix, o, p, s
	} else if prefix == 'y' {
		return prefix, s, o, p
	} else if prefix == 'z' {
		return prefix, p, s, o
	}
	log.Fatalln("Invalid minor permutation")
	return prefix, "", "", ""
}

// Mostly copied from https://github.com/piprate/json-gold/blob/master/ld/serialize_nquads.go
// serializes the elements of the quad and escapes values
// returns string (s, p, o) and []byte (graph)
func marshallQuad(quad *ld.Quad, cid string, index int) (string, string, string, []byte) {
	var s, p, o, g string

	// subject is either an IRI or blank node
	iri, isIRI := quad.Subject.(*ld.IRI)
	if isIRI {
		s = "<" + escape(iri.Value) + ">"
	} else {
		// Prefix blank nodes with the CID root
		s = cid + quad.Subject.GetValue()[1:]
	}

	// predicate is either an IRI or a blank node
	iri, isIRI = quad.Predicate.(*ld.IRI)
	if isIRI {
		p = "<" + escape(iri.Value) + ">"
	} else {
		// Prefix blank nodes with the CID root
		p = cid + quad.Predicate.GetValue()[1:]
	}

	// object is an IRI, blank node, or a literal
	iri, isIRI = quad.Object.(*ld.IRI)
	if isIRI {
		o = "<" + escape(iri.Value) + ">"
	} else if ld.IsBlankNode(quad.Object) {
		o = cid + escape(quad.Object.GetValue())[1:]
	} else {
		literal := quad.Object.(*ld.Literal)
		o = "\"" + escape(literal.GetValue()) + "\""
		if literal.Datatype == ld.RDFLangString {
			o += "@" + literal.Language
		} else if literal.Datatype != ld.XSDString {
			o += "^^<" + escape(literal.Datatype) + ">"
		}
	}

	g = cid
	if quad.Graph == nil {
		g += "\t" + string(index)
	} else if ld.IsIRI(quad.Graph) {
		g += "#" + quad.Graph.GetValue() + "\t" + string(index)
	} else if blankNode, isBlank := quad.Graph.(*ld.BlankNode); isBlank {
		g += blankNode.Attribute[1:] + "\t" + string(index)
	} else {
		log.Fatalln("Unexpected graph label", quad.Graph.GetValue())
	}

	return s, p, o, []byte(g + "\n")
}

// These are the escape rules for RDF strings.
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
