package styx

import (
	"encoding/binary"
	"log"
	"strings"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

var valuePrefixes = [3]byte{'a', 'b', 'c'}
var majorPrefixes = [3]byte{'i', 'j', 'k'}
var minorPrefixes = [3]byte{'x', 'y', 'z'}
var valuePrefixMap = map[byte]uint8{'a': 0, 'b': 1, 'c': 2}
var majorPrefixMap = map[byte]uint8{'i': 0, 'j': 1, 'k': 2}
var minorPrefixMap = map[byte]uint8{'x': 0, 'y': 1, 'z': 2}
var tab = byte('\t')

func updateIndex(
	counter []byte,
	prefixes [3]byte,
	s string,
	p string,
	o string,
	permute func(prefix byte, s string, p string, o string) ([]byte, string, string, string),
	txn *badger.Txn,
) ([3]uint64, error) {
	var indices [3]uint64
	for i, meta := range prefixes {
		prefix, m, n, _ := permute(meta, s, p, o)
		key := assembleKey(prefix, m, n, nil)
		index, err := txn.Get(key)
		if err == badger.ErrKeyNotFound {
			indices[i] = initialCounter
		} else if err != nil {
			return indices, err
		} else if index.UserMeta() != meta {
			log.Fatalln("Mismatching meta tag in major index")
		} else {
			counter, err = index.ValueCopy(counter)
			if err != nil {
				return indices, err
			}
			indices[i] = binary.BigEndian.Uint64(counter)
		}

		binary.BigEndian.PutUint64(counter, indices[i]+1)
		err = txn.SetWithMeta(key, counter, meta)
		if err != nil {
			return indices, err
		}
	}
	return indices, nil
}

func insert(cid string, dataset *ld.RDFDataset, txn *badger.Txn) error {
	counter := make([]byte, 8)
	for _, graph := range dataset.Graphs {
		for i, quad := range graph {
			s, p, o, label := marshallQuad(quad, cid, i)

			majors, err := updateIndex(counter, majorPrefixes, s, p, o, permuteMajor, txn)
			if err != nil {
				return err
			}

			minors, err := updateIndex(counter, minorPrefixes, s, p, o, permuteMinor, txn)
			if err != nil {
				return err
			}

			// Sanity check that majors and minors have the same values
			for i := range valuePrefixes {
				if majors[i] != minors[i] {
					log.Fatalln("Mismatching major & minor index values", majors, minors)
				}
			}

			for i, valuePrefix := range valuePrefixes {
				prefix, m, n, value := permuteValue(valuePrefix, s, p, o)
				val := append(label, value...)
				binary.BigEndian.PutUint64(counter, majors[i]) // warning: opinion
				key := assembleKey(prefix, m, n, counter)
				err = txn.SetWithMeta(key, val, valuePrefix)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func assembleKey(prefix []byte, m string, n string, counter []byte) []byte {
	prefix = append(prefix, m...)
	prefix = append(prefix, tab)
	prefix = append(prefix, n...)
	if counter != nil {
		prefix = append(prefix, counter...)
	}
	return prefix
}

func permuteValue(valuePrefix byte, s string, p string, o string) ([]byte, string, string, string) {
	prefix := []byte{valuePrefix, tab}
	if valuePrefix == 'a' {
		return prefix, p, o, s
	} else if valuePrefix == 'b' {
		return prefix, o, s, p
	} else if valuePrefix == 'c' {
		return prefix, s, p, o
	}
	log.Fatalln("Invalid value permutation")
	return nil, "", "", ""
}

func permuteMajor(majorPrefix byte, s string, p string, o string) ([]byte, string, string, string) {
	prefix := []byte{majorPrefix, tab}
	if majorPrefix == 'i' {
		return prefix, p, o, s
	} else if majorPrefix == 'j' {
		return prefix, o, s, p
	} else if majorPrefix == 'k' {
		return prefix, s, p, o
	}
	log.Fatalln("Invalid major permutation")
	return nil, "", "", ""
}

func permuteMinor(minorPrefix byte, s string, p string, o string) ([]byte, string, string, string) {
	prefix := []byte{minorPrefix, tab}
	if minorPrefix == 'x' {
		return prefix, p, o, s
	} else if minorPrefix == 'y' {
		return prefix, o, s, p
	} else if minorPrefix == 'z' {
		return prefix, s, p, o
	}
	log.Fatalln("Invalid minor permutation")
	return nil, "", "", ""
}

// Mostly copied from https://github.com/piprate/json-gold/blob/master/ld/serialize_nquads.go
func marshallQuad(quad *ld.Quad, cid string, index int) (string, string, string, []byte) {
	s := quad.Subject
	p := quad.Predicate
	o := quad.Object
	g := quad.Graph

	var subject, predicate, object string

	// subject is either an IRI or blank node
	iri, isIRI := s.(*ld.IRI)
	if isIRI {
		subject = "<" + escape(iri.Value) + ">"
	} else {
		// Prefix blank nodes with the CID root
		subject = cid + s.GetValue()[1:]
	}

	// predicate is either an IRI or a blank node
	iri, isIRI = p.(*ld.IRI)
	if isIRI {
		predicate = "<" + escape(iri.Value) + ">"
	} else {
		// Prefix blank nodes with the CID root
		predicate = cid + p.GetValue()[1:]
	}

	// object is an IRI, blank node, or a literal
	iri, isIRI = o.(*ld.IRI)
	if isIRI {
		object = "<" + escape(iri.Value) + ">"
	} else if ld.IsBlankNode(o) {
		object = cid + escape(o.GetValue())[1:]
	} else {
		literal := o.(*ld.Literal)
		object = "\"" + escape(literal.GetValue()) + "\""
		if literal.Datatype == ld.RDFLangString {
			object += "@" + literal.Language
		} else if literal.Datatype != ld.XSDString {
			object += "^^<" + escape(literal.Datatype) + ">"
		}
	}

	graph := cid
	if g == nil {
		graph += "\t" + string(index)
	} else if ld.IsIRI(g) {
		graph += "#" + g.GetValue() + "\t" + string(index)
	} else if blankNode, isBlank := g.(ld.BlankNode); isBlank {
		graph += blankNode.Attribute[1:] + "\t" + string(index)
	} else {
		log.Fatalln("Unexpected graph label", g.GetValue())
	}

	return subject, predicate, object, []byte(graph + "\n")
}

// These are the escape rules for RDF strings.
// It's why we use tab to delimit sections of keys and values.
// The "tab" glyph is such a cute concept idk why she's not more popular
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
