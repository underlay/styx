package types

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	multihash "github.com/multiformats/go-multihash"
	ld "github.com/piprate/json-gold/ld"
)

const tail = "zkiKpvWP3HqVQEfLDhexQzHj4sN413x"

func makeHashlinkURI(mh multihash.Multihash, fragment string) string {
	return fmt.Sprintf("hl:%s:%s%s", mh.B58String(), tail, fragment)
}

func makeDwebURI(mh multihash.Multihash, fragment string) string {
	c := cid.NewCidV1(cid.Raw, mh)
	return fmt.Sprintf("dweb:/ipfs/%s%s", c.String(), fragment)
}

func makeUlURI(mh multihash.Multihash, fragment string) string {
	c := cid.NewCidV1(cid.Raw, mh)
	return fmt.Sprintf("ul:/ipfs/%s%s", c.String(), fragment)
}

// var MakeURI = makeDWEBURI

// MakeURI turns a multihash instance into a URI
var MakeURI = makeHashlinkURI

var fragment = "(#(?:_:[a-zA-Z0-9]+)?)?"
var testUlURI = regexp.MustCompile(fmt.Sprintf("^ul:\\/ipfs\\/([a-zA-Z0-9]+)%s$", fragment))
var testDwebURI = regexp.MustCompile(fmt.Sprintf("^dweb:\\/ipfs\\/([a-zA-Z0-9]+)%s$", fragment))
var testHashlinkURI = regexp.MustCompile(fmt.Sprintf("^hl:([a-zA-Z0-9]+):%s%s$", tail, fragment))

// TestURI tests URIs
var TestURI = testHashlinkURI

func parseUlURI(uri string) (multihash.Multihash, string) {
	match := testUlURI.FindStringSubmatch(uri)
	c, _ := cid.Decode(match[1])
	return c.Hash(), match[2]
}

func parseDwebURI(uri string) (multihash.Multihash, string) {
	match := testDwebURI.FindStringSubmatch(uri)
	c, _ := cid.Decode(match[1])
	return c.Hash(), match[2]
}

func parseHashlinkURI(uri string) (multihash.Multihash, string) {
	match := testHashlinkURI.FindStringSubmatch(uri)
	mh, _ := multihash.FromB58String(match[1])
	return mh, match[2]
}

// ParseURI parses URIs
var ParseURI = parseHashlinkURI

// GetValue serializes the statement to a string.
func (statement *Statement) GetValue(valueMap ValueMap, txn *badger.Txn) string {
	if value, err := valueMap.Get(statement.Origin, txn); err == nil {
		if mh, err := multihash.Cast(value.GetDataset()); err == nil {
			return MakeURI(mh, fmt.Sprintf("#/%d", statement.GetIndex()))
		}
	}
	return ""
}

// Value needs to satisfy the ld.Node interface, which means implementing
// GetValue() => string and Equal(ld.Node) => bool

var patternInteger = regexp.MustCompile("^[\\-+]?[0-9]+$")
var patternDouble = regexp.MustCompile("^(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee](\\+|-)?[0-9]+)?$")

// ToJSON casts the value into the appropriate JSON-LD interface
func (value *Value) ToJSON(valueMap ValueMap, txn *badger.Txn) (r interface{}) {
	if blank := value.GetBlank(); blank != nil {
		if v, err := valueMap.Get(blank.Origin, txn); err == nil {
			if mh, err := multihash.Cast(v.GetDataset()); err == nil {
				uri := MakeURI(mh, fmt.Sprintf("#%s", blank.Id))
				r = map[string]interface{}{"@id": uri}
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
func (value *Value) GetValue(valueMap ValueMap, txn *badger.Txn) string {
	if blank := value.GetBlank(); blank != nil {
		if v, err := valueMap.Get(blank.Origin, txn); err != nil {
			if mh, err := multihash.Cast(v.GetDataset()); err != nil {
				return fmt.Sprintf("<%s>", MakeURI(mh, fmt.Sprintf("#%s", blank.Id)))
			}
		}
	} else if i, isIri := value.Node.(*Value_Iri); isIri {
		return fmt.Sprintf("<%s>", i.Iri)
	} else if l := value.GetLiteral(); l != nil {
		escaped := escape(l.Value)
		if l.Datatype == ld.RDFLangString {
			return fmt.Sprintf("\"%s\"@%s", escaped, l.Language)
		} else if l.Datatype != "" && l.Datatype != ld.XSDString {
			return fmt.Sprintf("\"%s\"^^<%s>", escaped, l.Datatype)
		} else {
			return fmt.Sprintf("\"%s\"", escaped)
		}
	}
	return ""
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
func (value *Value) Equal(node ld.Node) bool {
	return value.GetValue() == node.GetValue()
}

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
func (indexMap IndexMap) Get(node ld.Node, txn *badger.Txn) (*Index, error) {
	value := NodeToValue(0, node).GetValue(nil, txn)
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
func NodeToValue(origin uint64, node ld.Node) *Value {
	value := &Value{}
	if iri, isIri := node.(*ld.IRI); isIri {
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
// The Value struct is generated by protobuf.
type ValueMap map[uint64]*Value

// Commit writes the contents of the value map to badger
func (values ValueMap) Commit(txn *badger.Txn) (err error) {
	var val []byte
	for id, v := range values {
		if val, err = proto.Marshal(v); err != nil {
			return err
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

func (values ValueMap) Get(id uint64, txn *badger.Txn) (*Value, error) {
	if value, has := values[id]; has {
		return value, nil
	}
	key := make([]byte, 9)
	key[0] = ValuePrefix
	binary.BigEndian.PutUint64(key[1:], id)
	value := &Value{}
	if item, err := txn.Get(key); err != nil {
		return nil, err
	} else if val, err := item.ValueCopy(nil); err != nil {
		return nil, err
	} else {
		return value, proto.Unmarshal(val, value)
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
func PrintSources(statements []*Statement, valueMap ValueMap, txn *badger.Txn) string {
	s := "[ "
	for i, statement := range statements {
		if i > 0 {
			s += " | "
		}
		if value, err := valueMap.Get(statement.GetOrigin(), txn); err == nil {
			if mh, err := multihash.Cast(value.GetDataset()); err == nil {
				uri := MakeURI(mh, fmt.Sprintf("#/%d", statement.GetIndex()))
				s += fmt.Sprintf("%s (%s)", uri, statement.GetGraph())
			}
		}
	}
	return s + " ]"
}
