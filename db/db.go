package db

import (
	"bytes"
	"encoding/binary"
	"log"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	query "github.com/underlay/styx/query"
	types "github.com/underlay/styx/types"
)

// SequenceBandwidth sets the lease block size of the ID counter
const SequenceBandwidth = 512

// DB is the general styx database wrapper
type DB struct {
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
func OpenDB(path string) (*DB, error) {
	log.Println("Opening badger database at", path)
	opts := badger.DefaultOptions(path)
	if db, err := badger.Open(opts); err != nil {
		return nil, err
	} else if seq, err := db.GetSequence(types.SequenceKey, SequenceBandwidth); err != nil {
		return nil, err
	} else {
		return &DB{
			Badger:   db,
			Sequence: seq,
		}, nil
	}
}

// IngestJSONLd takes a JSON-LD document and ingests it.
// This is mostly a convenience method for testing;
// actual messages should get handled at HandleMessage.
func (db *DB) IngestJSONLd(doc interface{}, loader ld.DocumentLoader, store DocumentStore) error {
	options := GetStringOptions(loader)

	proc := ld.NewJsonLdProcessor()

	normalized, err := proc.Normalize(doc, options)
	if err != nil {
		return err
	}

	cid, err := store(bytes.NewReader([]byte(normalized.(string))))
	if err != nil {
		return err
	}

	quads, graphs, err := ParseMessage(bytes.NewReader([]byte(normalized.(string))))
	if err != nil {
		return err
	}

	return db.Badger.Update(func(txn *badger.Txn) (err error) {
		for label, graph := range graphs {
			if len(graph) == 0 {
				continue
			} else if err = db.insert(cid, quads, label, graph, txn); err != nil {
				return
			}
		}
		return
	})
}

// Ingest inserts a specific graph into the database
func (db *DB) Ingest(cid cid.Cid, quads []*ld.Quad, label string, graph []int) error {
	return db.Badger.Update(func(txn *badger.Txn) (err error) {
		return db.insert(cid, quads, label, graph, txn)
	})
}

// Query the database for a single result set
func (db *DB) Query(
	quads []*ld.Quad,
	label string,
	graph []int,
	data chan map[string]*types.Value,
	prov chan map[int]*types.SourceList,
) (err error) {
	return db.Badger.View(func(txn *badger.Txn) (err error) {
		d := map[string]*types.Value{}
		var s map[int]*types.SourceList
		defer func() {
			data <- d
			prov <- s
		}()

		var g *query.ConstraintGraph
		g, err = query.MakeConstraintGraph(quads, label, graph, nil, txn)
		defer g.Close()
		if err != nil {
			return
		}

		if err = g.Solve(txn); err != nil {
			return
		}

		if s, err = g.GetSources(); err != nil {
			return
		}

		var item *badger.Item
		var val []byte
		ids := map[uint64]*types.Value{}
		for p, u := range g.Index {
			// Translate u.Value into an RDF term string and save it to v
			id := binary.BigEndian.Uint64(u.Value)
			if value, has := ids[id]; has {
				d[p] = value
			} else {
				d[p] = &types.Value{}
				ids[id] = d[p]

				key := make([]byte, 9)
				key[0] = types.ValuePrefix
				copy(key[1:9], u.Value)

				if item, err = txn.Get(key); err != nil {
					return
				} else if val, err = item.ValueCopy(nil); err != nil {
					return
				} else if err = proto.Unmarshal(val, ids[id]); err != nil {
					return
				}
			}
		}
		return
	})
}

// Enumerate multiple result sets
func (db *DB) Enumerate(
	quads []*ld.Quad,
	label string,
	graph []int,
	extent int,
	domain map[string]ld.Node,
	data chan map[string]*types.Value,
	prov chan map[int]*types.SourceList,
) (err error) {
	return db.Badger.View(func(txn *badger.Txn) (err error) {
		d := make([]map[string]*types.Value, extent)
		sources := make([]map[int]*types.SourceList, extent)
		defer func() {
			for _, v := range d {
				data <- v
			}
			for _, s := range sources {
				prov <- s
			}
		}()

		var g *query.ConstraintGraph
		g, err = query.MakeConstraintGraph(quads, label, graph, domain, txn)
		defer g.Close()
		if err != nil {
			return
		}

		if err = g.Solve(txn); err != nil {
			return
		}

		var results [][][]byte
		if results, err = g.Collect(extent, sources, txn); err != nil {
			return
		}

		ids := map[uint64]*types.Value{}
		var item *badger.Item
		var val []byte
		for x, r := range results {
			d[x] = map[string]*types.Value{}
			for i, p := range g.Slice {
				// Translate u.Value into an RDF term string and save it to v
				id := binary.BigEndian.Uint64(r[i])
				if value, has := ids[id]; has {
					d[x][p] = value
				} else {
					d[x][p] = &types.Value{}
					ids[id] = d[x][p]

					key := make([]byte, 9)
					key[0] = types.ValuePrefix
					copy(key[1:9], r[i])

					if item, err = txn.Get(key); err != nil {
						return
					} else if val, err = item.ValueCopy(nil); err != nil {
						return
					} else if err = proto.Unmarshal(val, ids[id]); err != nil {
						return
					}
				}
			}
		}
		return
	})
}

// Ls lists the graphs in the database
func (db *DB) Ls(index string, extent int, messages chan []byte, dates chan []byte) error {
	var prefix = make([]byte, 1)
	prefix[0] = 'g'

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

	seek := make([]byte, 1+len(index))
	seek[0] = 'g'
	copy(seek[1:], index)

	return db.Badger.View(func(txn *badger.Txn) (err error) {
		iter := txn.NewIterator(iteratorOptions)
		defer iter.Close()

		i := 0
		for iter.Seek(seek); iter.Valid() && i < extent; iter.Next() {
			item := iter.Item()

			// Get the key
			size := item.KeySize() - 1
			key := make([]byte, size)
			copy(key, item.Key()[1:])

			// Key the value
			size = item.ValueSize()
			value := make([]byte, size)
			if value, err = item.ValueCopy(value); err != nil {
				return
			}

			messages <- key
			dates <- value

			i++
		}

		for ; i < extent; i++ {
			messages <- nil
			dates <- nil
		}

		return
	})
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
