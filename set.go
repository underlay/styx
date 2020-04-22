package styx

import (
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
	rdf "github.com/underlay/go-rdfjs"
)

// SetJSONLD sets a JSON-LD document
func (s *Store) SetJSONLD(uri string, input interface{}, canonize bool) error {
	var node rdf.Term = rdf.Default
	if uri != "" {
		node = rdf.NewNamedNode(uri)
	}

	opts := ld.NewJsonLdOptions(uri)
	dataset, err := getDataset(input, opts)
	if err != nil {
		return err
	}

	if canonize {
		na := ld.NewNormalisationAlgorithm(Algorithm)
		na.Normalize(dataset)

		quads := []*rdf.Quad{}
		for _, quad := range na.Quads() {
			quads = append(quads, fromLdQuad(quad, ""))
		}

		return s.Set(node, quads)
	}
	return s.Set(node, fromLdDataset(dataset, ""))
}

// Set is the entrypoint to inserting stuff
func (s *Store) Set(node rdf.Term, dataset []*rdf.Quad) (err error) {
	if node.TermType() == rdf.NamedNodeType {
		uri := node.Value()
		if strings.Index(uri, "#") != -1 || !s.Config.TagScheme.Test(uri+"#") {
			return ErrTagScheme
		}
	}

	dictionary := s.Config.Dictionary.Open(true)
	txn := s.Badger.NewTransaction(true)
	defer func() { txn.Discard(); dictionary.Commit() }()

	uc := newUnaryCache()
	bc := newBinaryCache()

	origin, err := dictionary.GetID(node, rdf.Default)
	if err != nil {
		return
	}

	quads, err := s.Config.QuadStore.Get(origin)
	if err != nil && err != ErrNotFound {
		return
	} else if quads != nil {
		txn, err = deleteQuads(origin, quads, dictionary, txn, s.Badger)
		if err != nil {
			return
		}
	}

	quads = make([][4]ID, len(dataset))

	var terms [3]ID
	var id ID
	var item *badger.Item
	var val []byte
	for i, quad := range dataset {
		source := &Statement{
			Base:  iri(origin),
			Index: uint64(i),
		}

		for j := Permutation(0); j < 4; j++ {
			id, err = dictionary.GetID(quad[j], node)
			if err != nil {
				return
			}

			quads[i][j] = id

			if j < 3 {
				terms[j] = id
			} else if t := quad[j].TermType(); t == rdf.BlankNodeType || t == rdf.DefaultGraphType {
				source.Graph, err = dictionary.GetID(quad[j], rdf.Default)
				if err != nil {
					return
				}
			} else {
				source.Graph = id
			}
		}

		for p := Permutation(0); p < 3; p++ {
			a, b, c := major.permute(p, terms)
			key := assembleKey(TernaryPrefixes[p], false, a, b, c)
			item, err = txn.Get(key)
			if err == badger.ErrKeyNotFound {
				// Since this is a new key we have to increment two binary keys.
				ab, ba := p, ((p+1)%3)+3
				err = bc.Increment(ab, a, b, uc, txn)
				if err != nil {
					return
				}
				err = bc.Increment(ba, b, a, uc, txn)
				if err != nil {
					return
				}
				if p == 0 {
					val = []byte(source.String())
				}
				txn, err = setSafe(key, val, txn, s.Badger)
				if err != nil {
					return
				}
			} else if err != nil {
				return
			} else if p == 0 {
				val, err = item.ValueCopy(nil)
				if err != nil {
					return
				}
				val = append(val, source.String()...)
				txn, err = setSafe(key, val, txn, s.Badger)
				if err != nil {
					return
				}
			}
		}
	}

	txn, err = bc.Commit(s.Badger, txn)
	if err != nil {
		return
	}

	txn, err = uc.Commit(s.Badger, txn)
	if err != nil {
		return
	}

	err = txn.Commit()
	if err != nil {
		return
	}

	return s.Config.QuadStore.Set(origin, quads)
}
