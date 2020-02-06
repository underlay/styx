package types

import (
	"encoding/binary"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/underlay/json-gold/ld"
)

// A Term is an RDF term serialized as a string, as defined by the N-Quads spec.
// Terms have chevrons around IRIs and use the "^^" and "@" syntax for literals.
type Term string

// Iri is an RDF IRI
type Iri string

// Cid is a Dataset reference
type Cid cid.Cid

// ValueType is an enum of Value types
type ValueType uint8

const (
	// IriType is the ValueType for Iri values
	IriType ValueType = iota
	// LiteralType is the ValueType for Literal values
	LiteralType
	// BlankType is the ValueType for Blank values
	BlankType
	// DatasetType is the ValueType for Cid values
	DatasetType
)

// Value is something that has a JSON-LD and N-Quads representation
type Value interface {
	JSON(values valueCache, uri URI, txn *badger.Txn) interface{}
	Term(values ValueCache, uri URI, txn *badger.Txn) Term
}

// JSON returns a JSON-LD value for the iri, satisfying the Value interface
func (iri Iri) JSON(values valueCache, uri URI, txn *badger.Txn) interface{} {
	return map[string]interface{}{"@id": string(iri)}
}

// Term returns the n-quads term for the iri, satisfying the Value interface
func (iri Iri) Term(values ValueCache, uri URI, txn *badger.Txn) Term {
	return Term(fmt.Sprintf("<%s>", iri))
}

// JSON returns a JSON-LD value for the dataset, satisfying the Value interface
func (d Cid) JSON(values valueCache, uri URI, txn *badger.Txn) interface{} {
	return map[string]interface{}{"@id": uri.String(cid.Cid(d), "")}
}

// Term returns the n-quads term for the dataset, satisfying the Value interface
func (d Cid) Term(values ValueCache, uri URI, txn *badger.Txn) Term {
	return Term(fmt.Sprintf("<%s>", uri.String(cid.Cid(d), "")))
}

// JSON returns a JSON-LD value for the blank node, satisfying the Value interface
func (blank *Blank) JSON(values valueCache, uri URI, txn *badger.Txn) (r interface{}) {
	if v, err := values.Get(blank.Origin, txn); err == nil {
		if d, is := v.(Cid); is {
			fragment := fmt.Sprintf("#%s", blank.Id)
			r = map[string]interface{}{
				"@id": uri.String(cid.Cid(d), fragment),
			}
		}
	}
	return
}

// Term returns the n-quads term for the blank node, satisfying the Value interface
func (blank *Blank) Term(values ValueCache, uri URI, txn *badger.Txn) Term {
	if v, err := values.Get(blank.Origin, txn); err == nil {
		if d, is := v.(Cid); is {
			c := cid.Cid(d)
			root := uri.String(c, "#"+blank.Id)
			return Term(fmt.Sprintf("<%s>", root))
		}
	}
	return ""
}

// JSON returns a JSON-LD value for the literal, satisfying the Value interface
func (literal *Literal) JSON(values valueCache, uri URI, txn *badger.Txn) (r interface{}) {
	v, d, l := literal.Value, literal.Datatype, literal.Language
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
	return
}

// Term returns the n-quads term for the literal, satisfying the Value interface
func (literal *Literal) Term(values ValueCache, uri URI, txn *badger.Txn) Term {
	escaped := escape(literal.Value)
	if literal.Datatype == ld.RDFLangString {
		return Term(fmt.Sprintf("\"%s\"@%s", escaped, literal.Language))
	} else if literal.Datatype != "" && literal.Datatype != ld.XSDString {
		return Term(fmt.Sprintf("\"%s\"^^<%s>", escaped, literal.Datatype))
	} else {
		return Term(fmt.Sprintf("\"%s\"", escaped))
	}
}

// GetValue serializes the statement to a string.
func (statement *Statement) GetValue(values valueCache, txn *badger.Txn, uri URI) (iri Iri) {
	value, err := values.Get(statement.Origin, txn)
	if c, is := value.(Cid); is && err == nil {
		iri = Iri(uri.String(cid.Cid(c), fmt.Sprintf("#/%d", statement.GetIndex())))
	}
	return
}

var patternInteger = regexp.MustCompile("^[\\-+]?[0-9]+$")
var patternDouble = regexp.MustCompile("^(\\+|-)?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)([Ee](\\+|-)?[0-9]+)?$")

// ValueToNode converts a value back to an ld.Node
func ValueToNode(value Value, values ValueCache, uri URI, txn *badger.Txn) (node ld.Node) {
	switch v := value.(type) {
	case Iri:
		node = ld.NewIRI(string(v))
	case *Blank:
		v2, err := values.Get(v.GetOrigin(), txn)
		if err != nil {
			return
		}
		if d, is := v2.(Cid); is {
			c := cid.Cid(d)
			node = ld.NewIRI(uri.String(c, "#"+v.Id))
		}
	case *Literal:
		value := v.GetValue()
		datatype := v.GetDatatype()
		language := v.GetLanguage()
		node = ld.NewLiteral(value, datatype, language)
	}
	return
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

// ID satisfies the Node interface for index values by returning the index Id.
// The Index struct is generated by protobuf.
func (index *Index) ID(_ interface{}) uint64 {
	return index.GetId()
}

// Increment the counter at the given position
func (index *Index) Increment(place Permutation) {
	switch place {
	case S:
		index.Subject++
	case P:
		index.Predicate++
	case O:
		index.Object++
	}
}

// Decrement the counter at the given position
func (index *Index) Decrement(place Permutation) {
	switch place {
	case S:
		index.Subject--
	case P:
		index.Predicate--
	case O:
		index.Object--
	}
}

// Get the counter at the given position
func (index *Index) Get(place Permutation) (counter uint64) {
	switch place {
	case S:
		counter = index.GetSubject()
	case P:
		counter = index.GetPredicate()
	case O:
		counter = index.GetObject()
	}
	return
}

// An IndexCache associates marshalled string values with the
// Index structs that have already been read from the database.
// The Index struct is generated by protobuf.
type IndexCache interface {
	Commit(db *badger.DB, txn *badger.Txn) (*badger.Txn, error)
	Get(term Term, txn *badger.Txn) (*Index, error)
	Set(term Term, index *Index)
}

type indexCache map[Term]*Index

// NewIndexCache creates a new IndexCache
func NewIndexCache() IndexCache {
	return indexCache{}
}

func (indices indexCache) Set(value Term, index *Index) {
	indices[value] = index
}

// Commit writes the contents of the index map to badger
func (indices indexCache) Commit(db *badger.DB, txn *badger.Txn) (*badger.Txn, error) {
	for v, index := range indices {
		key := AssembleKey(IndexPrefix, []byte(v), nil, nil)
		val, err := proto.Marshal(index)
		if err != nil {
			return nil, err
		}
		txn, err = SetSafe(key, val, txn, db)
		if err != nil {
			return nil, err
		}
	}
	return txn, nil
}

// Get memoizes database lookup for RDF nodes.
func (indices indexCache) Get(term Term, txn *badger.Txn) (*Index, error) {
	if index, has := indices[term]; has {
		return index, nil
	}

	key := AssembleKey(IndexPrefix, []byte(term), nil, nil)
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	index := &Index{}
	err = item.Value(func(val []byte) error { return proto.Unmarshal(val, index) })
	if err != nil {
		return nil, err
	}

	indices[term] = index
	return index, err
}

// NodeToValue parses a uint64 and an ld.Node into a Value struct.
func NodeToValue(node ld.Node, origin uint64, uri URI, txn *badger.Txn) Value {
	switch n := node.(type) {
	case *ld.IRI:
		if uri.Test(n.Value) {
			if c, fragment := uri.Parse(n.Value); c != cid.Undef && fragment != "" {
				key := AssembleKey(DatasetPrefix, c.Bytes(), nil, nil)
				item, err := txn.Get(key)
				if err != nil {
					dataset := &Dataset{}
					err = item.Value(func(val []byte) error { return proto.Unmarshal(val, dataset) })
					if err != nil {
						return &Blank{Origin: dataset.GetId(), Id: fragment}
					}
				}
			}
		}
		return Iri(n.Value)
	case *ld.BlankNode:
		return &Blank{Origin: origin, Id: n.Attribute}
	case *ld.Literal:
		l := &Literal{Value: n.Value}
		if n.Datatype == ld.RDFLangString {
			l.Datatype = ld.RDFLangString
			l.Language = n.Language
		} else if n.Datatype != "" && n.Datatype != ld.XSDString {
			l.Datatype = n.Datatype
		}
		return l
	}
	return nil
}

// NodeToTerm turns a node into a Term
func NodeToTerm(node ld.Node, origin cid.Cid, uri URI) Term {
	switch n := node.(type) {
	case *ld.IRI:
		return Term(fmt.Sprintf("<%s>", n.Value))
	case *ld.BlankNode:
		return Term(fmt.Sprintf("<%s>", uri.String(origin, "#"+n.Attribute)))
	case *ld.Literal:
		escaped := escape(n.Value)
		if n.Datatype == ld.RDFLangString {
			return Term(fmt.Sprintf("\"%s\"@%s", escaped, n.Language))
		} else if n.Datatype != "" && n.Datatype != ld.XSDString {
			return Term(fmt.Sprintf("\"%s\"^^<%s>", escaped, n.Datatype))
		} else {
			return Term(fmt.Sprintf("\"%s\"", escaped))
		}
	default:
		return ""
	}
}

// ValueCache caches values
type ValueCache interface {
	Commit(db *badger.DB, txn *badger.Txn) (*badger.Txn, error)
	Set(id uint64, value Value)
	Get(id uint64, txn *badger.Txn) (Value, error)
}

// A valueCache associates uint64 ids with a value.
// The Value struct definition is generated by protobuf.
type valueCache map[uint64]Value

// NewValueCache creates a new ValueCache
func NewValueCache() ValueCache {
	return valueCache{}
}

// Commit writes the contents of the value cache to badger
func (values valueCache) Commit(db *badger.DB, txn *badger.Txn) (t *badger.Txn, err error) {
	var val []byte
	var meta ValueType
	t = txn
	for id, value := range values {
		switch v := value.(type) {
		case Iri:
			meta = IriType
			val = []byte(v)
		case *Blank:
			meta = BlankType
			val, err = proto.Marshal(v)
			if err != nil {
				return
			}
		case *Literal:
			meta = LiteralType
			val, err = proto.Marshal(v)
			if err != nil {
				return
			}
		case Cid:
			meta = DatasetType
			val = cid.Cid(v).Bytes()
		}

		key := make([]byte, 9)
		key[0] = ValuePrefix
		binary.BigEndian.PutUint64(key[1:], id)

		e := badger.NewEntry(key, val).WithMeta(byte(meta))
		err = t.SetEntry(e)
		if err == badger.ErrTxnTooBig {
			err = t.Commit()
			if err != nil {
				return nil, err
			}
			t = db.NewTransaction(true)
			err = t.SetEntry(e)
		}
		if err != nil {
			return
		}
	}

	// Okay now that we've written all the values, we increment the
	// Value counter by len(values)
	_, txn, err = Increment(ValueCountKey, uint64(len(values)), txn, db)
	return
}

// Set a Value in the ValueCache
func (values valueCache) Set(id uint64, value Value) {
	values[id] = value
}

// Get a Value from the ValueCache
func (values valueCache) Get(id uint64, txn *badger.Txn) (Value, error) {
	value, has := values[id]
	if has {
		return value, nil
	} else if txn == nil {
		return nil, badger.ErrDiscardedTxn
	}
	key := make([]byte, 9)
	key[0] = ValuePrefix
	binary.BigEndian.PutUint64(key[1:], id)
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	value, err = GetValue(item)
	if err != nil {
		return nil, err
	}
	values[id] = value
	return value, err
}

// GetValue returns the Value at the item
func GetValue(item *badger.Item) (value Value, err error) {
	meta := ValueType(item.UserMeta())
	err = item.Value(func(val []byte) (err error) {
		switch meta {
		case IriType:
			value = Iri(val)
		case BlankType:
			b := &Blank{}
			err = proto.Unmarshal(val, b)
			value = b
		case LiteralType:
			l := &Literal{}
			err = proto.Unmarshal(val, l)
			value = l
		case DatasetType:
			var c cid.Cid
			c, err = cid.Cast(val)
			value = Cid(c)
		}
		return
	})
	return
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
func PrintSources(statements []*Statement, values ValueCache, uri URI, txn *badger.Txn) string {
	s := "[ "
	for i, statement := range statements {
		if i > 0 {
			s += " | "
		}
		if value, err := values.Get(statement.GetOrigin(), txn); err == nil {
			if d, is := value.(Cid); is {
				fragment := fmt.Sprintf("#/%d", statement.GetIndex())
				s += fmt.Sprintf("%s (%s)", uri.String(cid.Cid(d), fragment), statement.GetGraph())
			}
		}
	}
	return s + " ]"
}

func SetSafe(key, val []byte, txn *badger.Txn, db *badger.DB) (*badger.Txn, error) {
	err := txn.Set(key, val)
	if err == badger.ErrTxnTooBig {
		err = txn.Commit()
		if err != nil {
			return nil, err
		}
		txn = db.NewTransaction(true)
		err = txn.Set(key, val)
	}
	return txn, err
}

func Increment(key []byte, delta uint64, txn *badger.Txn, db *badger.DB) (uint64, *badger.Txn, error) {
	item, err := txn.Get(key)
	val := make([]byte, 8)
	if err == badger.ErrKeyNotFound {
		binary.BigEndian.PutUint64(val, delta)
		t, err := SetSafe(key, val, txn, db)
		return delta, t, err
	} else if err != nil {
		return 0, nil, err
	} else {
		val, err = item.ValueCopy(val)
		if err != nil {
			return 0, nil, err
		}
		sum := delta + binary.BigEndian.Uint64(val)
		binary.BigEndian.PutUint64(val, sum)
		t, err := SetSafe(key, val, txn, db)
		return sum, t, err
	}
}

func Decrement(key []byte, delta uint64, txn *badger.Txn, db *badger.DB) (count uint64, t *badger.Txn, err error) {
	var item *badger.Item
	item, err = txn.Get(key)
	if err != nil {
		return
	}

	err = item.Value(func(val []byte) error {
		count = binary.BigEndian.Uint64(val) - delta
		return nil
	})
	if err != nil {
		return
	}

	if count > 0 {
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, count)
		t, err = SetSafe(key, val, txn, db)
	} else {
		count = 0
		err = txn.Delete(key)
		if err == badger.ErrTxnTooBig {
			err = txn.Commit()
			if err != nil {
				return
			}
			t = db.NewTransaction(true)
			err = t.Delete(key)
		} else if err != nil {
		} else {
			t = txn
		}
	}

	return
}
