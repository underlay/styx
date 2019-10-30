package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"strings"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	core "github.com/ipfs/interface-go-ipfs-core"
	"github.com/multiformats/go-multihash"
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
	Loader   ld.DocumentLoader
	API      core.UnixfsAPI
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
func OpenDB(path string, id string, loader ld.DocumentLoader, api core.UnixfsAPI) (*DB, error) {
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
			Path:     path,
			ID:       id,
			Loader:   loader,
			API:      api,
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
	if n, err := proc.Normalize(doc, options); err != nil {
		return err
	} else {
		normalized = n.(string)
	}

	size := len(normalized)

	resolved, err := db.API.Add(context.Background(), files.NewReaderFile(strings.NewReader(normalized)))
	if err != nil {
		return err
	}

	c := resolved.Cid()

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

	return db.Badger.Update(func(txn *badger.Txn) (err error) {
		origin, err := db.insertDataset(c, uint32(len(quads)), uint32(size), graphList, txn)
		for label, graph := range graphs {
			if len(graph) == 0 {
				continue
			} else if err = db.insertGraph(origin, quads, label, graph, txn); err != nil {
				return
			}
		}
		return
	})
}

func (db *DB) insertDataset(
	c cid.Cid, length uint32, size uint32, graphs []string, txn *badger.Txn,
) (origin uint64, err error) {
	datasetKey := types.AssembleKey(types.DatasetPrefix, c.Hash(), nil, nil)

	// Check to see if this document is already in the database
	if _, err = txn.Get(datasetKey); err != badger.ErrKeyNotFound {
		if err == nil {
			log.Println("Dataset already inserted")
		}
		return
	}

	if origin, err = db.Sequence.Next(); err != nil {
		return
	}

	dataset := &types.Dataset{Id: origin, Length: length, Size: size, Graphs: graphs}

	var val []byte
	if val, err = proto.Marshal(dataset); err != nil {
		return
	}
	err = txn.Set(datasetKey, val)
	return
}

// Query the database for a single result set
func (db *DB) Query(
	quads []*ld.Quad,
	label string,
	graph []int,
	variables chan []string,
	data chan map[string]*types.Value,
	prov chan map[int]*types.SourceList,
) (err error) {
	return db.Badger.View(func(txn *badger.Txn) (err error) {
		d := map[string]*types.Value{}
		var s map[int]*types.SourceList
		var slice []string

		defer func() {
			variables <- slice
			data <- d
			prov <- s
		}()

		var g *query.ConstraintGraph
		g, err = query.MakeConstraintGraph(quads, label, graph, nil, nil, txn)
		defer g.Close()
		if err != nil {
			return
		}

		slice = g.Slice

		if err = g.Solve(txn); err != nil {
			return
		}

		if s, err = g.GetSources(txn); err != nil {
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
	domain []string,
	index []ld.Node,
	variables chan []string,
	data chan map[string]*types.Value,
	prov chan map[int]*types.SourceList,
) (err error) {
	return db.Badger.View(func(txn *badger.Txn) (err error) {
		var slice []string
		d := make([]map[string]*types.Value, extent)
		sources := make([]map[int]*types.SourceList, extent)
		defer func() {
			variables <- slice
			for _, v := range d {
				data <- v
			}
			for _, s := range sources {
				prov <- s
			}
		}()

		var g *query.ConstraintGraph
		g, err = query.MakeConstraintGraph(quads, label, graph, domain, index, txn)
		defer g.Close()
		if err != nil {
			return
		}

		slice = g.Slice

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

// Delete a dataset from the database
func (db *DB) Delete(c *cid.Cid) (err error) {
	return
}

// Ls lists the datasets in the database
func (db *DB) Ls(index ld.Node, extent int, graphs chan string) error {
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

		var seek []byte
		if iri, is := index.(*ld.IRI); is && index != nil && types.TestURI.MatchString(iri.Value) {
			mh, fragment := types.ParseURI(iri.Value)
			if mh != nil && fragment == "" {
				seek = make([]byte, len(mh)+1)
				copy(seek[1:], mh)
			}
		}
		if seek == nil {
			seek = make([]byte, 1)
		}
		seek[0] = types.DatasetPrefix

		i := 0
		for iter.Seek(seek); iter.Valid() && i < extent; iter.Next() {
			item := iter.Item()

			// Get the key
			dataset := &types.Dataset{}
			if val, err := item.ValueCopy(nil); err != nil {
				return err
			} else if err = proto.Unmarshal(val, dataset); err != nil {
				return err
			} else if mh, err := multihash.Cast(item.KeyCopy(nil)[1:]); err != nil {
				return err
			} else {
				for _, graph := range dataset.GetGraphs() {
					// blank := &types.Value_Blank{Origin: dataset.GetId(), Id: graph}
					graphs <- types.MakeURI(mh, "#"+graph)
				}
			}

			if err = proto.Unmarshal(item.Key()[1:], blank); err != nil {
				return
			}

			i++
		}

		for ; i < extent; i++ {
			graphs <- nil
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
				log.Printf("Value: %02d %s\n", id, value.GetValue(valueMap, txn))
			} else if _, has := types.TriplePrefixMap[prefix]; has {
				// Value key
				sourceList := &types.SourceList{}
				proto.Unmarshal(val, sourceList)
				log.Printf("Triple entry: %s %02d | %02d | %02d :: %s\n",
					string(key[0]),
					binary.BigEndian.Uint64(key[1:9]),
					binary.BigEndian.Uint64(key[9:17]),
					binary.BigEndian.Uint64(key[17:25]),
					types.PrintSources(sourceList.GetSources(), valueMap, txn),
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
				if mh, err := multihash.Cast(key[1:]); err != nil {
					return err
				} else {
					log.Printf("Dataset entry: <%s>\n", types.MakeURI(mh))
				}
			}
			i++
		}
		log.Printf("Printed %02d database entries\n", i)
		return nil
	})
}
