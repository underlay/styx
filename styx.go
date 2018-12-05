package styx

import (
	"encoding/binary"
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
var permutations = [6]byte{'a', 'b', 'c', 'x', 'y', 'z'}
var tab = byte('\t')
const INITIAL_COUNTER uint64 = 1

func incrementFlag(flag []byte) []byte {
	i := binary.BigEndian.Uint64(flag)
	binary.BigEndian.PutUint64(flag, i+1)
	return flag
}

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
	fmt.Println("normalizedd")
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
		return insert(cid, dataset, txn)
	})
}

func insert(cid string, dataset *ld.RDFDataset, txn *badger.Txn) error {
	counter := make([]byte, 8)
	for _, graph := range dataset.Graphs {
		for i, quad := range graph {
			s, p, o, g := marshallQuad(quad, cid, i)
			for _, permutation := range permutations {
				prefix, a, b, c := permute(permutation, s, p, o)
				value := append([]byte(g), []byte(string(i))...)
				rootKey := append(prefix, []byte(a)...)
				rootLen := len(rootKey)
				indexKey := append(append(rootKey, '\n'), []byte(b)...)
				indexItem, err := txn.Get(indexKey)
				if err == badger.ErrKeyNotFound {
					binary.BigEndian.PutUint64(counter, INITIAL_COUNTER)
					err = txn.SetWithMeta(indexKey, counter, permutation)
					if err != nil {
						return err
					}
					indexKey[rootLen] = '\t'
					valueKey := append(append(indexKey, '\t'), counter...)
					err = txn.SetWithMeta(valueKey, value, permutation)
					if err != nil {
						return err
					}
					continue
				} else if err != nil {
					return err
				} else if indexItem.UserMeta() != permutation {
					log.Fatalln("Mismatching meta tag in index item")
				}

				counter, err = indexItem.ValueCopy(counter)
				if err != nil {
					return err
				}

				i := binary.BigEndian.Uint64(counter)
				binary.BigEndian.PutUint64(counter, i+1)

				valueKey := 

				// indexKey :=
				key, val := compile(permutation, s, p, o, i, g)
				fmt.Println("key!", string(key))
				flag, err := txn.Get(key)
				if err == nil {
					if flag.UserMeta() != permutation {
						log.Fatal("Bad meta tag in badger db")
					}
					nextFlag, err := flag.ValueCopy(nil)
					if err != nil {
						return err
					}
					nextFlag = incrementFlag(nextFlag)
					err = txn.SetWithMeta(key, nextFlag, permutation)
					if err != nil {
						return err
					}
					realKey := append(append(key, byte('\t')), nextFlag...)
					return txn.SetWithMeta(realKey, val, permutation)
				} else if err == badger.ErrKeyNotFound {
					nextFlag := make([]byte, 8)
					binary.BigEndian.PutUint64(nextFlag, 0)
					err = txn.SetWithMeta(key, nextFlag, permutation)
					if err != nil {
						return err
					}
					realKey := append(append(key, byte('\t')), nextFlag...)
					return txn.SetWithMeta(realKey, val, permutation)
				} else {
					return err
				}
			}
			fmt.Println(s, p, o, g)
		}
	}
	return nil
}

func permute(permutation byte, s string, p string, o string) ([]byte, string, string, string) {
	prefix := []byte{permutation, tab}
	if permutation == 'a' {
		return prefix, s, p, o
	} else if permutation == 'b' {
		return prefix, p, o, s
	} else if permutation == 'c' {
		return prefix, o, s, p
	} else if permutation == 'x' {
		return prefix, s, o, p
	} else if permutation == 'y' {
		return prefix, p, s, o
	} else if permutation == 'z' {
		return prefix, o, p, s
	}
	log.Fatal("invalid permutation")
	return nil, "", "", ""
}

// Indexing permutations is surprisingly tricky.
// We use {a b c x y z} for the six permutations of {s p o}:
// a, b, and c are the three _rotations_ of spo that preserve s->p->o->s->p->... order.
// x, y, and z are the reverse rotations that follow s<-p<-o<-s<-p<-... order.
// In all cases, the graph label stays fixed as the fourth element.
func compile(permutation byte, s string, p string, o string, i int, g string) ([]byte, []byte) {
	k := g + "\t" + string(i) + "\t"
	if permutation == 'a' {
		// a: sp:og
		return []byte("a\t" + s + "\t" + p), []byte(k + o)
	} else if permutation == 'b' {
		// b: po:sg
		return []byte("b\t" + p + "\t" + o), []byte(k + s)
	} else if permutation == 'c' {
		// c: os:pg
		return []byte("c\t" + o + "\t" + s), []byte(k + p)
	} else if permutation == 'x' {
		// x: so:pg
		return []byte("x\t" + s + "\t" + o), []byte(k + p)
	} else if permutation == 'y' {
		// y: ps:og
		return []byte("y\t" + p + "\t" + s), []byte(k + o)
	} else if permutation == 'z' {
		// z: op:sg
		return []byte("z\t" + o + "\t" + p), []byte(k + s)
	}
	log.Fatal("invalid permutation index", permutation)
	return nil, nil
}

// Mostly copied from https://github.com/piprate/json-gold/blob/master/ld/serialize_nquads.go
func marshallQuad(quad *ld.Quad, cid string, index int) (string, string, string, string) {
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
		graph = cid + "\t" + string(index) + "\t"
	} else if ld.IsIRI(g) {
		graph = cid + "#" + g.GetValue() + "\t" + string(index) + "\t"
	} else if blankNode, isBlank := g.(ld.BlankNode); isBlank {
		// Prefix blank nodes with the CID root
		graph = cid + blankNode.Attribute[1:] + "\t" + string(index) + "\t"
	} else {
		log.Fatalln("Unexpected graph node")
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
