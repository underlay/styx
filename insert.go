package styx

import (
	"encoding/binary"
	"fmt"
	"log"
	"regexp"
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
m, n         []byte  escaped quad elements, but in the order they appear in the key of an entry
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
type PermuteIndex func(uint8, string, string, string) (byte, []byte, []byte, []byte)

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
		indexItem, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			counterValues[permutation] = InitialCounter
		} else if err != nil {
			return counterValues, err
		} else if indexItem.UserMeta() != prefix {
			log.Fatalln("Mismatching meta tag in major index")
		} else {
			counter, err = indexItem.ValueCopy(counter)
			if err != nil {
				return counterValues, err
			}
			counterValues[permutation] = binary.BigEndian.Uint64(counter) + 1
		}

		binary.BigEndian.PutUint64(counter, counterValues[permutation])
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
				binary.BigEndian.PutUint64(counter, majorValues[permutation]) // warning: opinion
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

func assembleKey(prefix byte, m []byte, n []byte, counter []byte) []byte {
	length := 1 + 1 + len(m) + 1 + len(n)
	if counter != nil {
		length += 1 + len(counter)
	}
	key := make([]byte, 0, length)
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

func permuteValue(permutation uint8, s string, p string, o string) (byte, []byte, []byte, []byte) {
	var S, P, O []byte = []byte(s), []byte(p), []byte(o)
	prefix := ValuePrefixes[permutation]
	if prefix == 'a' {
		return prefix, P, O, S
	} else if prefix == 'b' {
		return prefix, O, S, P
	} else if prefix == 'c' {
		return prefix, S, P, O
	}
	log.Fatalln("Invalid value permutation")
	return prefix, nil, nil, nil
}

func permuteMajor(permutation uint8, s string, p string, o string) (byte, []byte, []byte, []byte) {
	var S, P, O []byte = []byte(s), []byte(p), []byte(o)
	prefix := MajorPrefixes[permutation]
	if prefix == 'i' {
		return prefix, P, O, S
	} else if prefix == 'j' {
		return prefix, O, S, P
	} else if prefix == 'k' {
		return prefix, S, P, O
	}
	log.Fatalln("Invalid major permutation")
	return prefix, nil, nil, nil
}

func permuteMinor(permutation uint8, s string, p string, o string) (byte, []byte, []byte, []byte) {
	var S, P, O []byte = []byte(s), []byte(p), []byte(o)
	prefix := MinorPrefixes[permutation]
	if prefix == 'x' {
		return prefix, O, P, S
	} else if prefix == 'y' {
		return prefix, S, O, P
	} else if prefix == 'z' {
		return prefix, P, S, O
	}
	log.Fatalln("Invalid minor permutation")
	return prefix, nil, nil, nil
}

// Mostly copied from https://github.com/piprate/json-gold/blob/master/ld/serialize_nquads.go
// serializes the elements of the quad and escapes values
// returns string (s, p, o) and []byte (graph)
func marshallQuad(quad *ld.Quad, cid string, index int) (string, string, string, []byte) {
	var s, p, o, g string

	// subject is either an IRI or blank node
	if iri, isIRI := quad.Subject.(*ld.IRI); isIRI {
		s = marshallIRI(iri)
	} else if blankNode, isBlankNode := quad.Subject.(*ld.BlankNode); isBlankNode {
		// Prefix blank nodes with the CID root
		s = marshallBlankNode(blankNode, cid)
	} else {
		log.Fatalln("subject is neither an IRI nor a blank node")
	}

	// predicate is either an IRI or a blank node
	if iri, isIRI := quad.Predicate.(*ld.IRI); isIRI {
		p = marshallIRI(iri)
	} else if blankNode, isBlankNode := quad.Predicate.(*ld.BlankNode); isBlankNode {
		// Prefix blank nodes with the CID root
		p = marshallBlankNode(blankNode, cid)
	} else {
		log.Fatalln("predicate is neither an IRI nor a blank node")
	}

	// object is an IRI, blank node, or a literal
	if iri, isIRI := quad.Object.(*ld.IRI); isIRI {
		o = marshallIRI(iri)
	} else if blankNode, isBlankNode := quad.Object.(*ld.BlankNode); isBlankNode {
		o = marshallBlankNode(blankNode, cid)
	} else if literal, isLiteral := quad.Object.(*ld.Literal); isLiteral {
		o = marshallLiteral(literal)
	} else {
		log.Fatalln("object is neither an IRI nor a blank node nor a literal value")
	}

	g = "dweb:/ipfs/" + cid
	indexString := fmt.Sprintf("%d", index)
	if quad.Graph == nil {
		g += "\t" + indexString
	} else if ld.IsIRI(quad.Graph) || ld.IsBlankNode(quad.Graph) {
		g += "#" + quad.Graph.GetValue() + "\t" + indexString
	} else {
		log.Fatalln("Unexpected graph label", quad.Graph.GetValue())
	}

	return s, p, o, []byte(g + "\n")
}

func marshallBlankNode(blankNode *ld.BlankNode, cid string) string {
	return "<dweb:/ipfs/" + cid + "#" + blankNode.Attribute + ">"
}

func marshallIRI(iri *ld.IRI) string {
	return "<" + escape(iri.Value) + ">"
}

func marshallLiteral(literal *ld.Literal) string {
	value := "\"" + escape(literal.Value) + "\""
	if literal.Datatype == ld.RDFLangString {
		value += "@" + literal.Language
	} else if literal.Datatype != ld.XSDString {
		value += "^^<" + escape(literal.Datatype) + ">"
	}
	return value
}

const (
	iri      = "^<([^>]*)>$"
	blank    = "^_:(?:[A-Za-z][A-Za-z0-9]*)$"
	plain    = "\"([^\"\\\\]*(?:\\\\.[^\"\\\\]*)*)\""
	datatype = "(?:\\^\\^<([^>]*)>)"
	language = "(?:@([a-z]+(?:-[a-zA-Z0-9]+)*))"
	literal  = "^" + plain + "(?:" + datatype + "|" + language + ")?$"
)

var iriRegex = regexp.MustCompile(iri)
var blankRegex = regexp.MustCompile(blank)
var literalRegex = regexp.MustCompile(literal)

func unmarshalValue(value []byte) ld.Node {
	var node ld.Node
	if iriRegex.Match(value) {
		end := len(value) - 1
		node = ld.NewIRI(unescape(string(value[1:end])))
	} else if blankRegex.Match(value) {
		node = ld.NewBlankNode(unescape(string(value)))
	} else if match := literalRegex.FindStringSubmatch(string(value)); len(match) > 1 {
		var value, datatype, language string
		value = unescape(match[1])
		if len(match) > 2 {
			datatype = unescape(match[2])
			if len(match) > 3 {
				language = unescape(match[3])
			}
		}

		if datatype == "" {
			if language == "" {
				datatype = ld.XSDString
			} else {
				datatype = ld.RDFLangString
			}
		}
		node = ld.NewLiteral(value, datatype, language)
	}
	return node
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
