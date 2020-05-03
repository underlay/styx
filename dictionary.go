package styx

import (
	"encoding/binary"
	"errors"
	"regexp"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	rdf "github.com/underlay/go-rdfjs"
)

// ErrInvalidTerm indicates that the given term was of an unexpected type
var ErrInvalidTerm = errors.New("Invalid term")

// ErrNotFound indicates that the given node was syntactically valid, but not present in the database.
var ErrNotFound = errors.New("Not found")

var vocabulary map[string]iri = map[string]iri{}

func init() {
	for i, value := range constants {
		id := fromUint64(uint64(i))
		vocabulary[value] = id
	}
}

// A DictionaryFactory instantiates dictionaries
type DictionaryFactory interface {
	Init(db *badger.DB, tags TagScheme) error
	Open(update bool) Dictionary
	Close() error
}

// A Dictionary is a scheme for serializing terms to and from strings
type Dictionary interface {
	GetID(term rdf.Term, origin rdf.Term) (ID, error)
	GetTerm(id ID, origin rdf.Term) (rdf.Term, error)
	Commit() error
}

type stringDictionaryFactory struct{}
type stringDictionary struct{}

// StringDictionary is a dictionary that that serializes terms
// to and from their full N-Quads string representation
var StringDictionary DictionaryFactory = stringDictionaryFactory{}

func (s stringDictionaryFactory) Init(*badger.DB, TagScheme) error { return nil }
func (s stringDictionaryFactory) Close() error                     { return nil }
func (s stringDictionaryFactory) Open(bool) Dictionary             { return &stringDictionary{} }

func (s stringDictionary) Commit() error { return nil }

func (s stringDictionary) GetID(term rdf.Term, origin rdf.Term) (ID, error) {
	t := term.TermType()
	if origin.TermType() == rdf.NamedNodeType {
		if t == rdf.BlankNodeType {
			return ID("<" + origin.Value() + "#" + term.Value() + ">"), nil
		} else if t == rdf.DefaultGraphType {
			return ID("<" + origin.Value() + "#>"), nil
		} else if t == rdf.VariableType {
			return ID("<" + origin.Value() + "?" + term.Value() + ">"), nil
		}
	}
	return ID(term.String()), nil
}

func (s stringDictionary) GetTerm(id ID, origin rdf.Term) (rdf.Term, error) {
	term, err := rdf.ParseTerm(string(id))
	if err != nil {
		return nil, err
	}

	if term.TermType() == rdf.NamedNodeType && origin.TermType() == rdf.NamedNodeType {
		base, value := origin.Value(), term.Value()
		if value == base+"#" {
			return rdf.Default, nil
		} else if strings.HasPrefix(value, base+"#") {
			return rdf.NewBlankNode(value[len(base)+1:]), nil
		} else if strings.HasPrefix(value, base+"?") && len(value) > len(base)+1 {
			return rdf.NewVariable(value[len(base)+1:]), nil
		}
	}
	return term, nil
}

// SequenceBandwidth sets the lease block size of the ID counter
const SequenceBandwidth = 512

type iriDictionaryFactory struct {
	tags     TagScheme
	db       *badger.DB
	sequence *badger.Sequence
}

type iriDictionary struct {
	update  bool
	factory *iriDictionaryFactory
	txn     *badger.Txn
	values  map[iri]string
	ids     map[string]iri
}

// IriDictionary is a dictionary that compresses IRIs to
// monotonically-issued base64 identifiers
var IriDictionary DictionaryFactory = &iriDictionaryFactory{}

func (factory *iriDictionaryFactory) Init(db *badger.DB, tags TagScheme) (err error) {
	factory.db = db
	factory.tags = tags

	txn := db.NewTransaction(true)
	defer txn.Discard()
	_, err = txn.Get(SequenceKey)
	if err == badger.ErrKeyNotFound {
		// Yay! Now we have to write an initial one
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, 128)
		err = txn.Set(SequenceKey, val)
		if err != nil {
			return
		}

		err = txn.Commit()
		if err != nil {
			return
		}
	} else if err != nil {
		return
	}

	factory.sequence, err = db.GetSequence(SequenceKey, SequenceBandwidth)
	if err != nil {
		return
	}

	return
}

func (factory *iriDictionaryFactory) Close() (err error) {
	if factory.sequence != nil {
		err = factory.sequence.Release()
	}
	return
}

func (factory *iriDictionaryFactory) Open(update bool) Dictionary {
	txn := factory.db.NewTransaction(update)
	d := &iriDictionary{
		txn:     txn,
		update:  update,
		values:  map[iri]string{"": ""},
		ids:     map[string]iri{"": ""},
		factory: factory,
	}

	for value, id := range vocabulary {
		d.values[id] = value
		d.ids[value] = id
	}

	return d
}

func (d *iriDictionary) getIRI(value string) (iri, error) {
	id, has := d.ids[value]
	if has {
		return id, nil
	}

	key := make([]byte, len(value)+1)
	key[0] = ValueToIDPrefix
	copy(key[1:], value)
	item, err := d.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		if d.factory.sequence != nil && d.update {
			next, err := d.factory.sequence.Next()
			if err != nil {
				return "", err
			}

			id = fromUint64(next)
			idKey := make([]byte, 1+len(id))
			idKey[0] = IDToValuePrefix
			copy(idKey[1:], id)
			d.txn, err = setSafe(idKey, []byte(value), d.txn, d.factory.db)
			if err != nil {
				return "", err
			}

			valueKey := make([]byte, 1+len(value))
			valueKey[0] = ValueToIDPrefix
			copy(valueKey[1:], value)
			d.txn, err = setSafe(valueKey, []byte(id), d.txn, d.factory.db)
			if err != nil {
				return "", err
			}
		} else {
			return "", ErrNotFound
		}
	} else if err != nil {
		return "", err
	} else {
		err = item.Value(func(val []byte) error { id = iri(val); return nil })
		if err != nil {
			return "", err
		}
	}

	d.ids[value] = id
	d.values[id] = value
	return id, nil
}

func (d *iriDictionary) GetID(term rdf.Term, origin rdf.Term) (ID, error) {
	var base string
	if origin.TermType() == rdf.NamedNodeType {
		base = origin.Value()
	}

	value := term.Value()
	switch term := term.(type) {
	case *rdf.NamedNode:
		if d.factory.tags.Test(value) {
			tag, fragment := d.factory.tags.Parse(value)
			id, err := d.getIRI(tag)
			if err != nil {
				return NIL, err
			}
			return ID(string(id) + "#" + fragment), nil
		}
		id, err := d.getIRI(value)
		return ID(id), err
	case *rdf.BlankNode:
		id, err := d.getIRI(base)
		if err != nil {
			return NIL, err
		}
		return ID(string(id) + "#" + value), nil
	case *rdf.Literal:
		escaped := "\"" + escape(value) + "\""
		datatype, language := term.Datatype(), term.Language()
		if datatype == nil || datatype.Equal(rdf.XSDString) {
			return ID(escaped), nil
		} else if datatype.Equal(rdf.RDFLangString) {
			return ID(escaped + "@" + language), nil
		} else {
			id, err := d.getIRI(datatype.Value())
			return ID(escaped + ":" + string(id)), err
		}
	case *rdf.DefaultGraph:
		id, err := d.getIRI(base)
		if err != nil {
			return NIL, err
		}
		return ID(id + "#"), nil
	case *rdf.Variable:
		id, err := d.getIRI(base)
		if err != nil {
			return NIL, err
		}
		return ID(string(id) + "?" + value), nil
	default:
		return NIL, ErrInvalidTerm
	}
}

func (d *iriDictionary) getValue(id iri) (string, error) {
	value, has := d.values[id]
	if has {
		return value, nil
	}

	key := make([]byte, 1+len(id))
	key[0] = IDToValuePrefix
	copy(key[1:], id)
	item, err := d.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return "", ErrNotFound
	} else if err != nil {
		return "", err
	}

	var val []byte
	val, err = item.ValueCopy(nil)
	if err != nil {
		return "", err
	}

	value = string(val)
	d.values[id] = value
	d.ids[value] = id
	return value, nil
}

var patternLiteral = regexp.MustCompile("^\"([^\"\\\\]*(?:\\\\.[^\"\\\\]*)*)\"")

func (d *iriDictionary) GetTerm(id ID, origin rdf.Term) (rdf.Term, error) {
	var base string
	if origin.TermType() == rdf.NamedNodeType {
		base = origin.Value()
	}

	s := string(id)

	// Literal?
	li := patternLiteral.FindStringIndex(s)
	if li != nil && li[0] == 0 {
		value := unescape(s[1 : li[1]-1])
		if len(s) == li[1] {
			return rdf.NewLiteral(value, "", nil), nil
		} else if s[li[1]] == ':' {
			datatype, err := d.getValue(iri(s[li[1]+1:]))
			if err != nil {
				return nil, err
			}
			return rdf.NewLiteral(value, "", rdf.NewNamedNode(datatype)), nil
		} else if s[li[1]] == '@' {
			return rdf.NewLiteral(value, s[li[1]+1:], rdf.RDFLangString), nil
		} else {
			return nil, ErrInvalidTerm
		}
	}

	i, j := strings.IndexByte(s, '#'), strings.IndexByte(s, '?')

	// IRI?
	if i == -1 && j == -1 {
		t, err := d.getValue(iri(s))
		if err != nil {
			return nil, err
		}
		return rdf.NewNamedNode(t), nil
	}

	// Variable?
	if j != -1 {
		t, err := d.getValue(iri(s[:j]))
		if err != nil {
			return nil, err
		}
		value := s[j+1:]
		if t == base {
			return rdf.NewVariable(value), nil
		}
		return rdf.NewNamedNode(t + "?" + value), nil
	}

	// Blank node or default graph!
	t, err := d.getValue(iri(s[:i]))
	if err != nil {
		return nil, err
	}

	value := s[i+1:]
	if t == base {
		if value == "" {
			return rdf.Default, nil
		}
		return rdf.NewBlankNode(value), nil
	}
	return rdf.NewNamedNode(t + "#" + value), nil
}

func (d *iriDictionary) Commit() error {
	if d.txn == nil {
		return nil
	}

	if d.update {
		return d.txn.Commit()
	}

	d.txn.Discard()
	return nil
}
