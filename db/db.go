package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"strings"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	multihash "github.com/multiformats/go-multihash"
	ld "github.com/piprate/json-gold/ld"

	query "github.com/underlay/styx/query"
	types "github.com/underlay/styx/types"
)

// SequenceBandwidth sets the lease block size of the ID counter
const SequenceBandwidth = 512

// DB is the general styx database wrapper
type DB struct {
	Path     string
	ID       string
	uri      types.URI
	Loader   ld.DocumentLoader
	Store    DocumentStore
	Badger   *badger.DB
	Sequence *badger.Sequence
}

// Close the database
func (db *DB) Close() error {
	if err := db.Sequence.Release(); err != nil {
		return err
	}
	return db.Badger.Close()
}

// OpenDB opens a styx database
func OpenDB(path string, id string, loader ld.DocumentLoader, store DocumentStore) (*DB, error) {
	if path == "" {
		path = DefaultPath
	}

	log.Println("Opening badger database at", path)
	opts := badger.DefaultOptions(path)
	if db, err := badger.Open(opts); err != nil {
		return nil, err
	} else if seq, err := db.GetSequence(types.SequenceKey, SequenceBandwidth); err != nil {
		return nil, err
	} else {
		return &DB{
			uri:      types.UlURI,
			Path:     path,
			ID:       id,
			Loader:   loader,
			Store:    store,
			Badger:   db,
			Sequence: seq,
		}, nil
	}
}

// IngestJSONLd takes a JSON-LD document and ingests it.
// This is mostly a convenience method for testing;
// actual messages should get handled at HandleMessage.
func (db *DB) IngestJSONLd(doc interface{}) error {
	options := GetStringOptions(db.Loader)

	proc := ld.NewJsonLdProcessor()

	var normalized string
	n, err := proc.Normalize(doc, options)
	if err != nil {
		return err
	}
	normalized = n.(string)

	size := len(normalized)

	reader := strings.NewReader(normalized)
	mh, err := db.Store.Put(reader)
	if err != nil {
		return err
	}

	quads, graphs, err := ParseMessage(strings.NewReader(normalized))
	if err != nil {
		return err
	}

	graphList := make([]string, len(graphs))
	var i int
	for label := range graphs {
		graphList[i] = label
		i++
	}

	return db.Badger.Update(func(txn *badger.Txn) error {
		indexMap := types.IndexMap{}
		valueMap := types.ValueMap{}
		origin, err := db.insertDataset(mh, uint32(len(quads)), uint32(size), graphList, valueMap, txn)
		if err != nil {
			return err
		}

		for label, graph := range graphs {
			if len(graph) == 0 {
				continue
			}

			err := db.insertGraph(origin, quads, label, graph, indexMap, valueMap, txn)
			if err != nil {
				return err
			}
		}

		err = indexMap.Commit(txn)
		if err != nil {
			return err
		}
		return valueMap.Commit(txn)
	})
}

// Query the database!
func (db *DB) Query(
	quads []*ld.Quad,
	graph []int,
	domain []string,
	index []ld.Node,
	extent int,
	variables chan []string,
	data chan []ld.Node,
	prov chan query.Prov,
) (err error) {
	if extent == 0 {
		return nil
	}

	return db.Badger.View(func(txn *badger.Txn) (err error) {
		defer close(variables)
		defer close(data)
		defer close(prov)

		var g *query.ConstraintGraph
		g, err = query.MakeConstraintGraph(quads, graph, domain, index, db.uri, txn)
		defer g.Close()
		if err != nil {
			return
		}

		variables <- g.Domain

		valueMap := types.ValueMap{}
		fmt.Println("Okay we're starting iteration", g.Domain)
		for i := 0; i < extent; i++ {
			fmt.Println("i", i)
			tail, p, err := g.Next(txn)
			fmt.Println("Got next", tail, p, err)
			if err != nil {
				return err
			} else if tail == nil {
				break
			}
			d := make([]ld.Node, len(tail))
			for j, t := range tail {
				if t != nil {
					id := binary.BigEndian.Uint64(t)
					value, err := valueMap.Get(id, txn)
					if err != nil {
						return err
					}
					d[j] = types.ValueToNode(value, valueMap, db.uri, txn)
				}
			}
			fmt.Println("About to send d and p", d, p)
			data <- d
			prov <- p
			fmt.Println("Sent d and p")
		}
		return
	})
}

// Rm deletes a dataset from the database
func (db *DB) Rm(mh multihash.Multihash) (err error) {
	return
}

// Ls lists the datasets in the database
func (db *DB) Ls(index multihash.Multihash, extent int, datasets chan string) error {
	var prefix = make([]byte, 1)
	prefix[0] = types.DatasetPrefix

	prefetchSize := extent
	if prefetchSize > 100 {
		prefetchSize = 100
	}

	iteratorOptions := badger.IteratorOptions{
		PrefetchValues: true,
		PrefetchSize:   prefetchSize,
		Reverse:        false,
		AllVersions:    false,
		Prefix:         prefix,
	}

	return db.Badger.View(func(txn *badger.Txn) (err error) {
		iter := txn.NewIterator(iteratorOptions)
		defer iter.Close()
		defer close(datasets)

		var seek []byte
		if index != nil {
			seek = make([]byte, len(index)+1)
			copy(seek[1:], index)
		} else {
			seek = make([]byte, 1)
		}

		seek[0] = types.DatasetPrefix

		i := 0
		for iter.Seek(seek); iter.Valid() && i < extent; iter.Next() {
			item := iter.Item()

			// Get the key
			mh, err := multihash.Cast(item.KeyCopy(nil)[1:])
			if err != nil {
				return err
			}

			datasets <- db.uri.String(mh, "")
			i++
		}

		return
	})
}

// Log will print the *entire database contents* to log
func (db *DB) Log() error {
	return db.Badger.View(func(txn *badger.Txn) error {
		valueMap := types.ValueMap{}
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
			if bytes.Equal(key, types.SequenceKey) {
				// Counter!
				log.Printf("Sequence: %02d\n", binary.BigEndian.Uint64(val))
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
				log.Printf("Value: %02d %s\n", id, value.GetValue(valueMap, db.uri, txn))
			} else if _, has := types.TriplePrefixMap[prefix]; has {
				// Value key
				sourceList := &types.SourceList{}
				proto.Unmarshal(val, sourceList)
				log.Printf("Triple entry: %s %02d | %02d | %02d :: %s\n",
					string(key[0]),
					binary.BigEndian.Uint64(key[1:9]),
					binary.BigEndian.Uint64(key[9:17]),
					binary.BigEndian.Uint64(key[17:25]),
					types.PrintSources(sourceList.GetSources(), valueMap, db.uri, txn),
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
			} else if prefix == types.DatasetPrefix {
				mh, err := multihash.Cast(key[1:])
				if err != nil {
					return err
				}
				log.Printf("Dataset entry: <%s>\n", db.uri.String(mh, ""))
			}
			i++
		}
		log.Printf("Printed %02d database entries\n", i)
		return nil
	})
}
