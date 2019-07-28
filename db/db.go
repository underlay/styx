package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	query "github.com/underlay/styx/query"
	types "github.com/underlay/styx/types"
)

// DB is the general styx database wrapper
type DB struct {
	Badger *badger.DB
	Loader ld.DocumentLoader
}

// Pinner is anything that takes a string and returns a CID
type Pinner = func(nquads []byte) (cid.Cid, error)

// Close the Shit
func (db *DB) Close() error {
	return db.Badger.Close()
}

// OpenDB opens a styx database
func OpenDB(path string) (*DB, error) {
	opts := badger.DefaultOptions(path)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	// seq, err := db.GetSequence(types.CounterKey, bandwidth uint64)

	return &DB{
		Badger: db,
	}, nil
}

// IngestJSONLd takes a JSON-LD document and ingests it
func (db *DB) IngestJSONLd(doc interface{}, loader ld.DocumentLoader, store DocumentStore) error {
	datasetOptions := GetDatasetOptions(loader)
	stringOptions := GetStringOptions(loader)

	proc := ld.NewJsonLdProcessor()
	api := ld.NewJsonLdApi()

	rdf, err := proc.Normalize(doc, datasetOptions)
	if err != nil {
		return err
	}

	normalized, err := api.Normalize(rdf.(*ld.RDFDataset), stringOptions)
	if err != nil {
		return err
	}

	cid, err := store(bytes.NewReader([]byte(normalized.(string))))
	if err != nil {
		return err
	}

	return db.IngestDataset(cid, rdf.(*ld.RDFDataset))
}

// IngestDataset inserts every graph into the database (DON'T USE THIS IF YOU EXPECT QUERIES)
func (db *DB) IngestDataset(cid cid.Cid, dataset *ld.RDFDataset) error {
	err := db.Badger.Update(func(txn *badger.Txn) (err error) {
		for graph, quads := range dataset.Graphs {
			if graph == "@default" {
				graph = ""
			}

			if err = insert(cid, graph, quads, txn); err != nil {
				return
			}
		}
		return
	})
	return err
}

// IngestGraph inserts a graph into the database
func (db *DB) IngestGraph(cid cid.Cid, graph string, quads []*ld.Quad) error {
	if graph == "@default" {
		graph = ""
	}

	return db.Badger.Update(func(txn *badger.Txn) (err error) {
		return insert(cid, graph, quads, txn)
	})
}

// Query the database
func (db *DB) Query(quads []*ld.Quad, result chan []*ld.Quad) error {
	defer func() { result <- nil }()
	return db.Badger.View(func(txn *badger.Txn) (err error) {
		g, err := query.MakeConstraintGraph(quads, txn)
		defer g.Close()
		if err != nil {
			return
		}

		fmt.Println(g)

		if err = g.Solve(txn); err != nil {
			return
		}

		values := map[uint64]*types.Value{}
		for _, quad := range quads {
			quad.Subject, err = setValues(quad.Subject, g, values, txn)
			if err != nil {
				return err
			}

			quad.Predicate, err = setValues(quad.Predicate, g, values, txn)
			if err != nil {
				return err
			}

			quad.Object, err = setValues(quad.Object, g, values, txn)
			if err != nil {
				return err
			}
		}

		result <- quads
		return nil
	})
}

func setValues(node ld.Node, g *query.ConstraintGraph, values map[uint64]*types.Value, txn *badger.Txn) (ld.Node, error) {
	blank, isBlank := node.(*ld.BlankNode)
	if !isBlank {
		return node, nil
	}

	u := g.Index[blank.Attribute]
	id := binary.BigEndian.Uint64(u.Value)

	if value, has := values[id]; has {
		return value, nil
	}

	key := make([]byte, 9)
	key[0] = types.ValuePrefix
	copy(key[1:9], u.Value)
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	buf, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	value := &types.Value{}
	err = proto.Unmarshal(buf, value)
	if err != nil {
		return nil, err
	}

	values[id] = value
	return value, nil
}

// Log will print the *entire database contents* to log
func (db *DB) Log() error {
	return db.Badger.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		var i int
		for iter.Seek(nil); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.KeyCopy(nil)
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			prefix := key[0]
			if bytes.Equal(key, types.CounterKey) {
				// Counter!
				log.Printf("Counter: %02d\n", binary.BigEndian.Uint64(val))
			} else if prefix == types.IndexPrefix {
				// Index key
				index := &types.Index{}
				if err = proto.Unmarshal(val, index); err != nil {
					return err
				}
				log.Printf("Index:\n  %s\n  %s\n", string(key[1:]), index.String())
			} else if prefix == types.ValuePrefix {
				// Value key
				value := &types.Value{}
				if err = proto.Unmarshal(val, value); err != nil {
					return err
				}
				id := binary.BigEndian.Uint64(key[1:])
				log.Printf("Value: %02d %s\n", id, value.GetValue())
			} else if _, has := types.TriplePrefixMap[prefix]; has {
				// Value key
				sourceList := &types.SourceList{}
				proto.Unmarshal(val, sourceList)
				log.Printf("Triple entry: %s %02d | %02d | %02d :: %s\n",
					string(key[0]),
					binary.BigEndian.Uint64(key[1:9]),
					binary.BigEndian.Uint64(key[9:17]),
					binary.BigEndian.Uint64(key[17:25]),
					types.PrintSources(sourceList.Sources),
				)
			} else if _, has := types.MinorPrefixMap[prefix]; has {
				// Minor key
				log.Printf("Minor entry: %s %02d | %02d :: %02d\n",
					string(key[0]),
					binary.BigEndian.Uint64(key[1:9]),
					binary.BigEndian.Uint64(key[9:17]),
					binary.BigEndian.Uint64(val),
				)
			} else if _, has := types.MajorPrefixMap[prefix]; has {
				// Major key
				log.Printf("Major entry: %s %02d | %02d :: %02d\n",
					string(key[0]),
					binary.BigEndian.Uint64(key[1:9]),
					binary.BigEndian.Uint64(key[9:17]),
					binary.BigEndian.Uint64(val),
				)
			}
			i++
		}
		log.Printf("Printed %02d database entries\n", i)
		return nil
	})
}
