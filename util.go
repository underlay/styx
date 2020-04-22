package styx

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"regexp"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
	rdf "github.com/underlay/go-rdfjs"
)

type ID string

var NIL ID = ""

type iri string

var proc = ld.NewJsonLdProcessor()

var patternInteger = regexp.MustCompile("^[\\-+]?[0-9]+$")
var patternDouble = regexp.MustCompile("^(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee](\\+|-)?[0-9]+)?$")

const max6Byte uint64 = 16777216
const max8Byte uint64 = 281474976710656

func fromUint64(id uint64) iri {
	var res []byte
	if id < max6Byte {
		res = make([]byte, 4)
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, uint32(id))
		base64.StdEncoding.Encode(res, tmp[1:])
	} else if id < max8Byte {
		res = make([]byte, 8)
		tmp := make([]byte, 8)
		binary.BigEndian.PutUint64(tmp, id)
		base64.StdEncoding.Encode(res, tmp[2:])
	}
	l := len(res)
	if res[l-2] == '=' {
		return iri(res[:l-2])
	} else if res[l-1] == '=' {
		return iri(res[:l-1])
	} else {
		return iri(res)
	}
}

func escape(str string) string {
	str = strings.Replace(str, "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "\n", "\\n", -1)
	str = strings.Replace(str, "\r", "\\r", -1)
	str = strings.Replace(str, "\t", "\\t", -1)
	return str
}

func unescape(str string) string {
	str = strings.Replace(str, "\\\\", "\\", -1)
	str = strings.Replace(str, "\\\"", "\"", -1)
	str = strings.Replace(str, "\\n", "\n", -1)
	str = strings.Replace(str, "\\r", "\r", -1)
	str = strings.Replace(str, "\\t", "\t", -1)
	return str
}

// assembleKey concatenates the passed slices
func assembleKey(prefix byte, tail bool, terms ...ID) []byte {
	l := 0
	for _, term := range terms {
		l += 1 + len(term)
	}
	if tail {
		l++
	}
	key := make([]byte, l)
	key[0] = prefix
	i := 1
	for _, term := range terms {
		copy(key[i:i+len(term)], term)
		i += len(term)
		if i < l {
			key[i] = '\t'
			i++
		}
	}
	return key
}

// setSafe writes the entry and returns a new transaction if the old one was full.
func setSafe(key, val []byte, txn *badger.Txn, db *badger.DB) (*badger.Txn, error) {
	e := badger.NewEntry(key, val).WithMeta(key[0])
	err := txn.SetEntry(e)
	if err == badger.ErrTxnTooBig {
		err = txn.Commit()
		if err != nil {
			return nil, err
		}
		txn = db.NewTransaction(true)
		err = txn.SetEntry(e)
	}
	return txn, err
}

// deleteSafe deletes the entry and returns a new transaction if the old one was full.
func deleteSafe(key []byte, txn *badger.Txn, db *badger.DB) (*badger.Txn, error) {
	err := txn.Delete(key)
	if err == badger.ErrTxnTooBig {
		err = txn.Commit()
		if err != nil {
			return nil, err
		}
		txn = db.NewTransaction(true)
		err = txn.Delete(key)
	}
	return txn, err
}

// matrix is a type for 3x3 permutators
type matrix [3][3]uint8

// permute permutes the given ids by the specified permutation
func (m matrix) permute(permutation Permutation, ids [3]ID) (ID, ID, ID) {
	row := m[permutation]
	return ids[row[0]], ids[row[1]], ids[row[2]]
}

// major indexes the major permutations
var major = matrix{
	[3]uint8{0, 1, 2},
	[3]uint8{1, 2, 0},
	[3]uint8{2, 0, 1},
}

// minor indexes the minor permutations
var minor = matrix{
	[3]uint8{0, 2, 1},
	[3]uint8{1, 0, 2},
	[3]uint8{2, 1, 0},
}

func getDataset(input interface{}, opts *ld.JsonLdOptions) (dataset *ld.RDFDataset, err error) {
	var document interface{}
	switch input := input.(type) {
	case []byte:
		err = json.Unmarshal(input, &document)
	case string:
		err = json.Unmarshal([]byte(input), &document)
	case io.Reader:
		err = json.NewDecoder(input).Decode(&document)
	case map[string]interface{}:
		document = input
	case []interface{}:
		document = input
	default:
		err = ErrInvalidInput
	}
	if err != nil {
		return
	}

	var rdf interface{}
	rdf, err = proc.ToRDF(document, opts)
	if err != nil {
		return
	}

	switch result := rdf.(type) {
	case *ld.RDFDataset:
		return result, err
	default:
		err = ErrInvalidInput
	}

	return
}

func fromLdDataset(dataset *ld.RDFDataset, base string) []*rdf.Quad {
	result := []*rdf.Quad{}
	for _, quads := range dataset.Graphs {
		for _, quad := range quads {
			result = append(result, fromLdQuad(quad, base))
		}
	}
	return result
}

func fromLdQuad(quad *ld.Quad, base string) *rdf.Quad {
	return rdf.NewQuad(
		fromLdNode(quad.Subject, base),
		fromLdNode(quad.Predicate, base),
		fromLdNode(quad.Object, base),
		fromLdNode(quad.Graph, base),
	)
}

var blankNodePrefix = "_:"

func fromLdNode(node ld.Node, base string) rdf.Term {
	if node == nil {
		return rdf.Default
	}

	switch node := node.(type) {
	case *ld.IRI:
		if base != "" && strings.HasPrefix(node.Value, base) {
			return rdf.NewVariable(node.Value[len(base):])
		}
		return rdf.NewNamedNode(node.Value)
	case *ld.BlankNode:
		if node.Attribute == "" || node.Attribute == "@default" {
			return rdf.Default
		} else if node.Attribute[:len(blankNodePrefix)] == blankNodePrefix {
			return rdf.NewBlankNode(node.Attribute[len(blankNodePrefix):])
		} else {
			return rdf.NewBlankNode(node.Attribute)
		}
	case *ld.Literal:
		if node.Language != "" {
			return rdf.NewLiteral(node.Value, node.Language, rdf.RDFLangString)
		} else if node.Datatype != "" && node.Datatype != ld.XSDString {
			return rdf.NewLiteral(node.Value, "", rdf.NewNamedNode(node.Datatype))
		} else {
			return rdf.NewLiteral(node.Value, "", nil)
		}
	}
	return nil
}

func toLdNode(term rdf.Term) ld.Node {
	switch term := term.(type) {
	case *rdf.NamedNode:
		return ld.NewIRI(term.Value())
	case *rdf.BlankNode:
		return ld.NewBlankNode(blankNodePrefix + term.Value())
	case *rdf.Literal:
		if term.Datatype() == nil {
			return ld.NewLiteral(term.Value(), "", "")
		}
		return ld.NewLiteral(term.Value(), term.Datatype().Value(), term.Language())
	case *rdf.DefaultGraph:
		return ld.NewBlankNode("@default")
	default:
		return nil
	}
}

func toLdQuad(quad *rdf.Quad) *ld.Quad {
	return &ld.Quad{
		Subject:   toLdNode(quad.Subject()),
		Predicate: toLdNode(quad.Predicate()),
		Object:    toLdNode(quad.Object()),
		Graph:     toLdNode(quad.Graph()),
	}
}

func ToRDFDataset(quads []*rdf.Quad) *ld.RDFDataset {
	dataset := ld.NewRDFDataset()
	for _, quad := range quads {
		label := quad.Graph().String()
		if label == "" {
			label = "@default"
		}
		if graph, has := dataset.Graphs[label]; has {
			dataset.Graphs[label] = append(graph, toLdQuad(quad))
		} else {
			dataset.Graphs[label] = []*ld.Quad{toLdQuad(quad)}
		}
	}
	return dataset
}
