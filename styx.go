package styx

import (
	"fmt"
	"log"
	"strings"

	ipfs "github.com/ipfs/go-ipfs-api"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

// algorithm can be URDNA2015 or URGNA2012
const algorithm = "URDNA2015"

// format has to be application/nquads
const format = "application/nquads"

// These are explained later
var permutations = [6]string{"a", "b", "c", "x", "y", "z"}
var newline = []byte("\n")[0]

func ingest(doc interface{}, db *badger.DB, shell *ipfs.Shell) error {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(nil)

	// Convert to RDF
	rdf, err := proc.ToRDF(doc, options)
	if err != nil {
		return err
	}
	dataset := rdf.(*ld.RDFDataset)

	// Normalize and add to IFPS
	options.Format = format
	options.Algorithm = algorithm
	api := ld.NewJsonLdApi()
	normalized, err := api.Normalize(dataset, options)
	fmt.Println(normalized)
	if err != nil {
		return err
	}
	reader := strings.NewReader(normalized.(string))
	cid, err := shell.Add(reader)
	if err != nil {
		return err
	}
	return db.Update(func(txn *badger.Txn) error {
		for _, graph := range dataset.Graphs {
			for _, quad := range graph {
				s, p, o, g := marshallQuad(quad, cid)
				for _, permutation := range permutations {
					key, val := compile(permutation, s, p, o, g)
					meta := byte(permutation[0])
					item, err := txn.Get(key)
					if err == nil {
						if item.UserMeta() != meta {
							log.Fatal("Conflicting meta tag in badger db")
						}
						dst, err := item.ValueCopy(nil)
						if err != nil {
							return err
						}
						err = txn.SetWithMeta(key, append(dst, val...), meta)
						if err != nil {
							return err
						}
					} else if err == badger.ErrKeyNotFound {
						err = txn.SetWithMeta(key, val, meta)
						if err != nil {
							return err
						}
					} else {
						return err
					}
				}
				fmt.Println(s, p, o, g)
			}
		}
		return nil
	})
}

// Indexing permutations is surprisingly tricky.
// We use {a b c x y z} for the six permutations of {s p o}:
// a, b, and c are the three _rotations_ of spo that preserve s->p->o->s->p->... order.
// x, y, and z are the reverse rotations that follow s<-p<-o<-s<-p<-... order.
// In all cases, the graph label stays fixed as the fourth element.
func compile(permutation string, s string, p string, o string, g string) ([]byte, []byte) {
	if permutation == "a" {
		// a: sp:og
		return []byte("a\t" + s + "\t" + p), []byte(o + "\t" + g + "\n")
	} else if permutation == "b" {
		// b: po:sg
		return []byte("b\t" + p + "\t" + o), []byte(s + "\t" + g + "\n")
	} else if permutation == "c" {
		// c: os:pg
		return []byte("c\t" + o + "\t" + s), []byte(p + "\t" + g + "\n")
	} else if permutation == "x" {
		// x: so:pg
		return []byte("x\t" + s + "\t" + o), []byte(p + "\t" + g + "\n")
	} else if permutation == "y" {
		// y: ps:og
		return []byte("y\t" + p + "\t" + s), []byte(o + "\t" + g + "\n")
	} else if permutation == "z" {
		// z: op:sg
		return []byte("z\t" + o + "\t" + p), []byte(s + "\t" + g + "\n")
	}
	log.Fatal("invalid permutation index", permutation)
	return nil, nil
}

// Mostly copied from https://github.com/piprate/json-gold/blob/master/ld/serialize_nquads.go
func marshallQuad(quad *ld.Quad, cid string) (string, string, string, string) {
	s := quad.Subject
	p := quad.Predicate
	o := quad.Object
	g := quad.Graph

	var subject, predicate, object, graph string

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

	// graph is either an IRI or blank node
	if g == nil {
		graph = cid
	} else if ld.IsIRI(g) {
		graph = cid + "#" + g.GetValue()
	} else {
		// Prefix blank nodes with the CID root
		graph = cid + g.GetValue()[1:]
	}

	return subject, predicate, object, graph
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
