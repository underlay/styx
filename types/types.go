package types

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	"github.com/ipfs/go-cid"
	multibase "github.com/multiformats/go-multibase"
	ld "github.com/piprate/json-gold/ld"
)

const tail = "zkiKpvWP3HqVQEfLDhexQzHj4sN413x"

const fragment = "(#(?:_:c14n\\d+)?)?"

var testUlURI = regexp.MustCompile(fmt.Sprintf("^ul:\\/ipfs\\/([a-zA-Z0-9]{59})%s$", fragment))
var testDwebURI = regexp.MustCompile(fmt.Sprintf("^dweb:\\/ipfs\\/([a-zA-Z0-9]+)%s$", fragment))
var testHashlinkURI = regexp.MustCompile(fmt.Sprintf("^hl:([a-zA-Z0-9]+):%s%s$", tail, fragment))

type URI interface {
	Parse(uri string) (c cid.Cid, fragment string)
	String(c cid.Cid, fragment string) (uri string)
}

type hlURI struct{}

func (*hlURI) Parse(uri string) (c cid.Cid, fragment string) {
	if match := testHashlinkURI.FindStringSubmatch(uri); match != nil {
		_, mh, _ := multibase.Decode(match[1])
		c = cid.NewCidV1(cid.Raw, mh)
		fragment = match[2]
	}
	return
}

func (*hlURI) String(c cid.Cid, fragment string) (uri string) {
	s, _ := multibase.Encode(multibase.Base58BTC, c.Hash())
	return fmt.Sprintf("hl:%s:%s%s", s, tail, fragment)
}

var HlURI URI = (*hlURI)(nil)

type ulURI struct{}

func (*ulURI) Parse(uri string) (c cid.Cid, fragment string) {
	if match := testUlURI.FindStringSubmatch(uri); match != nil {
		c, _ = cid.Decode(match[1])
		fragment = match[2]
	}
	return
}

func (*ulURI) String(c cid.Cid, fragment string) (uri string) {
	s, _ := c.StringOfBase(multibase.Base32)
	return fmt.Sprintf("ul:/ipfs/%s%s", s, fragment)
}

var UlURI URI = (*ulURI)(nil)

func MakeFileURI(c cid.Cid) string {
	s, _ := c.StringOfBase(multibase.Base32)
	return fmt.Sprintf("dweb:/ipfs/%s", s)
}

// TestURI tests URIs
var TestURI = testHashlinkURI

// GetValue serializes the statement to a string.
func (statement *Statement) GetValue(valueMap ValueMap, txn *badger.Txn, uri URI) string {
	if value, err := valueMap.Get(statement.Origin, txn); err == nil {
		if c, err := cid.Cast(value.GetDataset()); err == nil {
			return uri.String(c, fmt.Sprintf("#/%d", statement.GetIndex()))
		}
	}
	return ""
}

// Value needs to satisfy the ld.Node interface, which means implementing
// GetValue() => string and Equal(ld.Node) => bool

var patternInteger = regexp.MustCompile("^[\\-+]?[0-9]+$")
var patternDouble = regexp.MustCompile("^(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee](\\+|-)?[0-9]+)?$")

// ToJSON casts the value into the appropriate JSON-LD interface
func (value *Value) ToJSON(valueMap ValueMap, uri URI, txn *badger.Txn) (r interface{}) {
	if blank := value.GetBlank(); blank != nil {
		if v, err := valueMap.Get(blank.Origin, txn); err == nil {
			if c, err := cid.Cast(v.GetDataset()); err == nil {
				r = map[string]interface{}{
					"@id": uri.String(c, fmt.Sprintf("#%s", blank.Id)),
				}
			}
		}
	} else if t, is := value.Node.(*Value_Iri); is {
		r = map[string]interface{}{"@id": t.Iri}
	} else if t := value.GetLiteral(); t != nil {
		v, d, l := t.Value, t.Datatype, t.Language
		if d == ld.RDFLangString {
			r = map[string]interface{}{"@value": v, "@language": l}
		} else if d == "" || d == ld.XSDString {
			r = v
		} else if d == ld.XSDBoolean {
			if v == "true" {
				r = true
			} else if v == "false" {
				r = false
			} else {
				r = map[string]interface{}{"@value": v, "@type": d}
			}
		} else if d == ld.XSDInteger && patternInteger.MatchString(v) {
			r, _ = strconv.Atoi(v)
		} else if d == ld.XSDDouble && patternDouble.MatchString(v) {
			r, _ = strconv.ParseFloat(v, 64)
		} else {
			r = map[string]interface{}{"@value": v, "@type": d}
		}
	}
	return
}

// GetValue serializes the value to a string.
func (value *Value) GetValue(valueMap ValueMap, uri URI, txn *badger.Txn) (s string) {
	if blank := value.GetBlank(); blank != nil {
		if v, err := valueMap.Get(blank.Origin, txn); err != nil {
		} else if c, err := cid.Cast(v.GetDataset()); err != nil {
		} else {
			s = fmt.Sprintf("<%s>", uri.String(c, fmt.Sprintf("#%s", blank.Id)))
		}
	} else if iri := value.GetIri(); iri != "" {
		s = fmt.Sprintf("<%s>", iri)
	} else if l := value.GetLiteral(); l != nil {
		escaped := escape(l.Value)
		if l.Datatype == ld.RDFLangString {
			s = fmt.Sprintf("\"%s\"@%s", escaped, l.Language)
		} else if l.Datatype != "" && l.Datatype != ld.XSDString {
			s = fmt.Sprintf("\"%s\"^^<%s>", escaped, l.Datatype)
		} else {
			s = fmt.Sprintf("\"%s\"", escaped)
		}
	} else if ds := value.GetDataset(); ds != nil {
		if c, err := cid.Cast(ds); err != nil {
		} else {
			s = fmt.Sprintf("<%s>", uri.String(c, ""))
		}
	}
	return
}

// ValueToNode converts a value back to an ld.Node
func ValueToNode(value *Value, valueMap ValueMap, uri URI, txn *badger.Txn) ld.Node {
	if blank := value.GetBlank(); blank != nil {
		if v, err := valueMap.Get(blank.GetOrigin(), txn); err == nil {
			if ds := v.GetDataset(); ds != nil {
				if c, err := cid.Cast(ds); err != nil {
					fragment := fmt.Sprintf("#%s", blank.GetId())
					return ld.NewIRI(uri.String(c, fragment))
				}
			}
		}
	} else if literal := value.GetLiteral(); literal != nil {
		value := literal.GetValue()
		datatype := literal.GetDatatype()
		language := literal.GetLanguage()
		return ld.NewLiteral(value, datatype, language)
	} else if iri := value.GetIri(); iri != "" {
		return ld.NewIRI(iri)
	}
	return nil
}

func escape(str string) string {
	str = strings.Replace(str, "\\", "\\\\", -1)
	str = strings.Replace(str, "\"", "\\\"", -1)
	str = strings.Replace(str, "\n", "\\n", -1)
	str = strings.Replace(str, "\r", "\\r", -1)
	str = strings.Replace(str, "\t", "\\t", -1)
	return str
}

// Equal tests for equality between two ld.Nodes
// func (value *Value) Equal(node ld.Node) bool {
// 	return value.GetValue() == node.GetValue()
// }

// GetID satisfies the HasValue interface for index values by returning the index Id.
// The Index struct is generated by protobuf.
func (index *Index) GetID(_ interface{}) uint64 {
	return index.GetId()
}

// Increment the counter at the given position
func (index *Index) Increment(place uint8) {
	if place == 0 {
		index.Subject++
	} else if place == 1 {
		index.Predicate++
	} else if place == 2 {
		index.Object++
	}
}

// Get the counter at the given position
func (index *Index) Get(place uint8) uint64 {
	if place == 0 {
		return index.GetSubject()
	} else if place == 1 {
		return index.GetPredicate()
	} else if place == 2 {
		return index.GetObject()
	} else {
		return 0
	}
}

// An IndexMap associates marshalled string values with the
// Index structs that have already been read from the database.
// The Index struct is generated by protobuf.
type IndexMap map[string]*Index

// Commit writes the contents of the index map to badger
func (indexMap IndexMap) Commit(txn *badger.Txn) (err error) {
	var val []byte
	for v, index := range indexMap {
		key := AssembleKey(IndexPrefix, []byte(v), nil, nil)
		if val, err = proto.Marshal(index); err != nil {
			return
		} else if err = txn.Set(key, val); err != nil {
			return
		}
	}
	return
}

// Get memoizes database lookup for RDF nodes.
func (indexMap IndexMap) Get(node ld.Node, uri URI, txn *badger.Txn) (*Index, error) {
	value := NodeToValue(node, 0, uri, txn).GetValue(nil, uri, txn)
	if index, has := indexMap[value]; has {
		return index, nil
	}

	key := AssembleKey(IndexPrefix, []byte(value), nil, nil)
	if item, err := txn.Get(key); err != nil {
		return nil, err
	} else if val, err := item.ValueCopy(nil); err != nil {
		return nil, err
	} else {
		indexMap[value] = &Index{}
		if err = proto.Unmarshal(val, indexMap[value]); err != nil {
			return nil, err
		}
		return indexMap[value], nil
	}
}

// NodeToValue parses a uint64 and an ld.Node into a Value struct.
func NodeToValue(node ld.Node, origin uint64, uri URI, txn *badger.Txn) *Value {
	value := &Value{}
	if iri, isIri := node.(*ld.IRI); isIri {
		if TestURI.MatchString(iri.Value) {
			if c, fragment := uri.Parse(iri.Value); fragment != "" {
				key := AssembleKey(DatasetPrefix, c.Bytes(), nil, nil)
				if item, err := txn.Get(key); err != nil {
					if val, err := item.ValueCopy(nil); err != nil {
						dataset := &Dataset{}
						if err = proto.Unmarshal(val, dataset); err != nil {
							b := &Value_Blank{Origin: dataset.GetId(), Id: fragment}
							value.Node = &Value_Blank_{Blank: b}
							return value
						}
					}
				}
			}
		}
		value.Node = &Value_Iri{Iri: iri.Value}
	} else if literal, isLiteral := node.(*ld.Literal); isLiteral {
		l := &Value_Literal{Value: literal.Value}
		if literal.Datatype == ld.RDFLangString {
			l.Datatype = ld.RDFLangString
			l.Language = literal.Language
		} else if literal.Datatype != "" && literal.Datatype != ld.XSDString {
			l.Datatype = literal.Datatype
		}
		value.Node = &Value_Literal_{Literal: l}
	} else if blank, isBlank := node.(*ld.BlankNode); isBlank {
		b := &Value_Blank{Origin: origin, Id: blank.Attribute}
		value.Node = &Value_Blank_{Blank: b}
	}
	return value
}

// A ValueMap associates uint64 ids with a value.
// The Value struct definition is generated by protobuf.
type ValueMap map[uint64]*Value

// Commit writes the contents of the value map to badger
func (valueMap ValueMap) Commit(txn *badger.Txn) (err error) {
	var val []byte
	for id, v := range valueMap {
		if val, err = proto.Marshal(v); err != nil {
			return
		}

		key := make([]byte, 9)
		key[0] = ValuePrefix
		binary.BigEndian.PutUint64(key[1:], id)

		if err = txn.Set(key, val); err != nil {
			return
		}
	}
	return
}

// Get a Value from the ValueMap
func (valueMap ValueMap) Get(id uint64, txn *badger.Txn) (*Value, error) {
	if value, has := valueMap[id]; has {
		return value, nil
	} else if txn == nil {
		return nil, badger.ErrDiscardedTxn
	}
	key := make([]byte, 9)
	key[0] = ValuePrefix
	binary.BigEndian.PutUint64(key[1:], id)
	v := &Value{}
	if item, err := txn.Get(key); err != nil {
		return nil, err
	} else if val, err := item.ValueCopy(nil); err != nil {
		return nil, err
	} else {
		err := proto.Unmarshal(val, v)
		valueMap[id] = v
		return v, err
	}
}

// AssembleKey will look at the prefix byte to determine
// how many of the elements {abc} to pack into the key.
func AssembleKey(prefix byte, a, b, c []byte) []byte {
	A, B, C := len(a), len(b), len(c)
	key := make([]byte, 1+A+B+C)
	key[0] = prefix
	if A > 0 {
		copy(key[1:1+A], a)
		if B > 0 {
			copy(key[1+A:1+A+B], b)
			if C > 0 {
				copy(key[1+A+B:1+A+B+C], c)
			}
		}
	}
	return key
}

// PrintSources pretty-prints a slice of sources on a single line.
func PrintSources(statements []*Statement, valueMap ValueMap, uri URI, txn *badger.Txn) string {
	s := "[ "
	for i, statement := range statements {
		if i > 0 {
			s += " | "
		}
		if value, err := valueMap.Get(statement.GetOrigin(), txn); err == nil {
			if c, err := cid.Cast(value.GetDataset()); err == nil {
				fragment := fmt.Sprintf("#/%d", statement.GetIndex())
				s += fmt.Sprintf("%s (%s)", uri.String(c, fragment), statement.GetGraph())
			}
		}
	}
	return s + " ]"
}
