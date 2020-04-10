package styx

import (
	"fmt"
	"regexp"
	"strconv"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// Term is the shorthand type for values (terms of any kind)
type Term string

type Statement struct {
	Origin iri
	Index  uint64
	Graph  Value
}

func (statement *Statement) URI(values valueCache, txn *badger.Txn) (uri string) {
	value, err := values.GetValue(statement.Origin, txn)
	if err == nil {
		uri = fmt.Sprintf("%s/%d", value, statement.Index)
	}
	return
}

func (statement *Statement) Marshal(values valueCache, txn *badger.Txn) string {
	i := strconv.FormatUint(statement.Index, 32)
	return fmt.Sprintf("%s\t%s\t%s\n", statement.Origin, i, statement.Graph.Term())
}

var statementPattern = regexp.MustCompile("([a-zA-Z0-9+/]+)\t([a-z0-9]+)\t([a-zA-Z0-9+/]+)(?:#([a-zA-Z0-9-_]+))?")

func getStatements(val []byte) ([]*Statement, error) {
	wahoo := statementPattern.FindAllSubmatch(val, -1)
	statements := make([]*Statement, len(wahoo))
	for i, w := range wahoo {
		index, err := strconv.ParseUint(string(w[2]), 32, 64)
		if err != nil {
			return nil, err
		}

		var graph Value
		if len(w[4]) == 0 {
			graph = iri(w[3])
		} else {
			graph = &blank{origin: iri(w[3]), id: string(w[4])}
		}

		statements[i] = &Statement{
			Origin: iri(w[1]),
			Index:  index,
			Graph:  graph,
		}
	}
	return statements, nil
}

// A Value is an RDF term
type Value interface {
	Term() Term
	Node(origin iri, values valueCache, txn *badger.Txn) ld.Node

	// We don't actually use JSON or NQuads but they might be nice to have in the future
	JSON(origin iri, values valueCache, txn *badger.Txn) interface{}
	NQuads(origin iri, values valueCache, txn *badger.Txn) string
}

func (i iri) Term() Term {
	return Term(i)
}

type iri string

// JSON returns a JSON-LD value for the iri, satisfying the Value interface
func (i iri) JSON(origin iri, cache valueCache, txn *badger.Txn) interface{} {
	value, err := cache.GetValue(i, txn)
	if err != nil {
		return nil
	}
	return map[string]interface{}{"@id": value}
}

// NQuads returns the n-quads term for the iri, satisfying the Value interface
func (i iri) NQuads(origin iri, cache valueCache, txn *badger.Txn) string {
	value, err := cache.GetValue(i, txn)
	if err == nil {
		return fmt.Sprintf("<%s>", value)
	}
	return ""
}

// Node returns an ld.Node for the ID
func (i iri) Node(origin iri, cache valueCache, txn *badger.Txn) ld.Node {
	value, err := cache.GetValue(i, txn)
	if err != nil {
		return nil
	}
	return ld.NewIRI(value)
}

type blank struct {
	origin iri
	id     string
}

func (b *blank) Term() Term {
	return Term(fmt.Sprintf("%s#%s", b.origin, b.id))
}

// JSON returns a JSON-LD value for the blank node, satisfying the Value interface
func (b *blank) JSON(origin iri, cache valueCache, txn *badger.Txn) (r interface{}) {
	var id string
	if b.origin == origin {
		id = "_:" + b.id
	} else {
		o, err := cache.GetValue(b.origin, txn)
		if err == nil {
			id = fmt.Sprintf("%s#_:%s", o, b.id)
		}
	}
	return map[string]interface{}{"@id": id}
}

// NQuads returns the n-quads term for the blank node, satisfying the Value interface
func (b *blank) NQuads(origin iri, cache valueCache, txn *badger.Txn) string {
	if b.origin == origin {
		return "_:" + b.id
	}
	o, err := cache.GetValue(b.origin, txn)
	if err == nil {
		return fmt.Sprintf("<%s#_:%s>", o, b.id)
	}
	return ""
}

func (b *blank) Node(origin iri, cache valueCache, txn *badger.Txn) ld.Node {
	if b.origin == origin {
		return ld.NewBlankNode("_:" + b.id)
	}
	o, err := cache.GetValue(b.origin, txn)
	if err != nil {
		return nil
	}
	return ld.NewIRI(fmt.Sprintf("%s#_:%s", o, b.id))
}

type literal struct {
	v string
	l string
	d iri
}

func (l *literal) Term() Term {
	escaped := []byte(escape(l.v))
	if l.d == RDFLangString {
		return Term(fmt.Sprintf("\"%s\"@%s", escaped, l.l))
	} else if l.d != "" && l.d != XSDString {
		return Term(fmt.Sprintf("\"%s\":%s", escaped, l.d))
	} else {
		return Term(fmt.Sprintf("\"%s\"", escaped))
	}
}

// JSON returns a JSON-LD value for the literal, satisfying the Value interface
func (l *literal) JSON(origin iri, cache valueCache, txn *badger.Txn) (r interface{}) {
	if l.d == RDFLangString {
		r = map[string]interface{}{"@value": l.v, "@language": l.l}
	} else if l.d == "" || l.d == XSDString {
		r = l.v
	} else if l.d == XSDBoolean && (l.v == "true" || l.v == "false") {
		r, _ = strconv.ParseBool(l.v)
	} else if l.d == XSDInteger && patternInteger.MatchString(l.v) {
		r, _ = strconv.Atoi(l.v)
	} else if l.d == XSDDouble && patternDouble.MatchString(l.v) {
		r, _ = strconv.ParseFloat(l.v, 64)
	} else {
		datatype, err := cache.GetValue(l.d, txn)
		if err == nil {
			r = map[string]interface{}{"@value": l.v, "@type": datatype}
		}
	}
	return
}

// NQuads returns the n-quads term for the literal, satisfying the Value interface
func (l *literal) NQuads(origin iri, cache valueCache, txn *badger.Txn) string {
	escaped := escape(l.v)
	if l.d == RDFLangString {
		return fmt.Sprintf("\"%s\"@%s", escaped, l.l)
	} else if l.d != "" && l.d != XSDString {
		value, err := cache.GetValue(l.d, txn)
		if err == nil {
			return fmt.Sprintf("\"%s\"^^<%s>", escaped, value)
		}
	} else {
		return fmt.Sprintf("\"%s\"", escaped)
	}
	return ""
}

func (l *literal) Node(origin iri, cache valueCache, txn *badger.Txn) ld.Node {
	if l.d == RDFLangString {
		return ld.NewLiteral(l.v, ld.RDFLangString, l.l)
	} else if l.d != "" && l.d != XSDString {
		value, err := cache.GetValue(l.d, txn)
		if err == nil {
			return ld.NewLiteral(l.v, value, "")
		}
	} else {
		return ld.NewLiteral(l.v, ld.XSDString, "")
	}
	return nil
}

func nodeToValue(
	node ld.Node,
	origin iri,
	cache valueCache,
	tag TagScheme,
	t *badger.Txn,
	sequence *badger.Sequence,
	db *badger.DB,
) (value Value, txn *badger.Txn, err error) {
	if node == nil {
		return &blank{origin: origin}, t, nil
	}

	txn = t
	switch node := node.(type) {
	case *ld.IRI:
		if tag.Test(node.Value) {
			uri, fragment := tag.Parse(node.Value)
			var origin iri
			origin, txn, err = getIRI(uri, cache, txn, sequence, db)
			value = &blank{origin, fragment}
		} else {
			value, txn, err = getIRI(node.Value, cache, txn, sequence, db)
		}
	case *ld.BlankNode:
		value = &blank{origin: origin, id: node.Attribute[2:]}
	case *ld.Literal:
		l := &literal{v: node.Value}
		if node.Datatype == ld.RDFLangString {
			l.d = RDFLangString
			l.l = node.Language
		} else if node.Datatype != "" && node.Datatype != ld.XSDString {
			l.d, txn, err = getIRI(node.Datatype, cache, txn, sequence, db)
		}
		value = l
	}
	return
}

func getIRI(
	value string,
	cache valueCache,
	t *badger.Txn,
	sequence *badger.Sequence,
	db *badger.DB,
) (id iri, txn *badger.Txn, err error) {
	txn = t
	id, err = cache.GetID(value, txn)
	if err == badger.ErrKeyNotFound && sequence != nil && db != nil {
		var next uint64
		next, err = sequence.Next()
		if err != nil {
			return
		}
		id = fromUint64(next)
		txn, err = cache.Commit(id, value, db, txn)
	}
	return
}
