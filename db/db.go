package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	core "github.com/ipfs/interface-go-ipfs-core"
	options "github.com/ipfs/interface-go-ipfs-core/options"
	ld "github.com/underlay/json-gold/ld"
	pkgs "github.com/underlay/pkgs/query"

	query "github.com/underlay/styx/query"
	types "github.com/underlay/styx/types"
)

// DefaultPath is the default path for the Badger database
const DefaultPath = "/tmp/styx"

// SequenceBandwidth sets the lease block size of the ID counter
const SequenceBandwidth = 512

// DB is the general styx database wrapper
type DB struct {
	Path     string
	uri      types.URI
	FS       core.UnixfsAPI
	Badger   *badger.DB
	Sequence *badger.Sequence
}

var _ types.Styx = (*DB)(nil)

// Close the database
func (db *DB) Close() (err error) {
	if db == nil {
		return
	}
	if db.Sequence != nil {
		err = db.Sequence.Release()
		if err != nil {
			return
		}
	}
	if db.Badger != nil {
		err = db.Badger.Close()
		if err != nil {
			return
		}
	}
	return
}

// OpenDB opens a styx database
func OpenDB(path string, uri types.URI) (types.Styx, error) {
	if path == "" {
		path = DefaultPath
	}

	if uri == nil {
		uri = types.UnderlayURI
	}

	log.Println("Opening badger database at", path)
	opts := badger.DefaultOptions(path)
	if db, err := badger.Open(opts); err != nil {
		return nil, err
	} else if seq, err := db.GetSequence(types.SequenceKey, SequenceBandwidth); err != nil {
		return nil, err
	} else {
		return &DB{
			uri:      uri,
			Path:     path,
			Badger:   db,
			Sequence: seq,
		}, nil
	}
}

// IngestJSONLd takes a JSON-LD document and ingests it.
// This is mostly a convenience method for testing;
// actual messages should get handled at HandleMessage.
func IngestJSONLd(db types.Styx, api core.CoreAPI, doc interface{}) (cid.Cid, []*ld.Quad, error) {
	proc := ld.NewJsonLdProcessor()
	opts := ld.NewJsonLdOptions("")
	opts.DocumentLoader = ld.NewDwebDocumentLoader(api)
	opts.Algorithm = types.Algorithm
	d, err := proc.ToRDF(doc, opts)
	if err != nil {
		return cid.Undef, nil, err
	}

	opts.Format = types.Format
	na := ld.NewNormalisationAlgorithm(opts.Algorithm)
	na.Normalize(d.(*ld.RDFDataset))
	nquads, err := na.Format(opts)
	if err != nil {
		return cid.Undef, nil, err
	}

	reader := strings.NewReader(nquads.(string))

	resolved, err := api.Unixfs().Add(
		context.Background(),
		files.NewReaderFile(reader),
		options.Unixfs.Pin(false),
		options.Unixfs.CidVersion(1),
		options.Unixfs.RawLeaves(true),
	)

	if err != nil {
		return cid.Undef, nil, err
	}

	c := resolved.Cid()
	quads := na.Quads()
	return c, quads, db.Insert(c, quads)
}

// Query satisfies the Styx interface
func (db *DB) Query(pattern []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (pkgs.Cursor, error) {
	txn := db.Badger.NewTransaction(false)
	g, err := query.MakeConstraintGraph(pattern, domain, index, db.uri, txn)
	if err != nil {
		g.Close()
		g = nil
	}

	if err == badger.ErrKeyNotFound {
		err = nil
	} else if err == query.ErrInitialCountZero {
		err = nil
	} else if err == query.ErrEmptyIntersect {
		err = nil
	} else if err == query.ErrEmptyJoin {
		err = nil
	}

	return g, err
}

// Delete removes a dataset from the database
func (db *DB) Delete(c cid.Cid, dataset []*ld.Quad) (err error) {
	txn := db.Badger.NewTransaction(true)
	defer func() { txn.Discard() }()

	datasetKey := types.AssembleKey(types.DatasetPrefix, c.Bytes(), nil, nil)
	item, err := txn.Get(datasetKey)
	if err == badger.ErrKeyNotFound {
		return nil
	} else if err != nil {
		return err
	}

	err = txn.Delete(datasetKey)
	if err != nil {
		return err
	}

	ds := &types.Dataset{}
	err = item.Value(func(val []byte) error { return proto.Unmarshal(val, ds) })
	if err != nil {
		return err
	}

	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, ds.Id)
	valueKey := types.AssembleKey(types.ValuePrefix, value, nil, nil)
	err = txn.Delete(valueKey)
	if err != nil {
		return err
	}

	_, txn, err = types.Decrement(types.DatasetCountKey, 1, txn, db.Badger)
	if err != nil {
		return err
	}

	_, txn, err = types.Decrement(types.TripleCountKey, uint64(len(dataset)), txn, db.Badger)
	if err != nil {
		return err
	}

	indexCache := types.NewIndexCache()
	for i, quad := range dataset {
		s := types.NodeToTerm(quad.Subject, c, db.uri)
		p := types.NodeToTerm(quad.Predicate, c, db.uri)
		o := types.NodeToTerm(quad.Object, c, db.uri)

		S, err := indexCache.Get(s, txn)
		if err != nil {
			return err
		}

		P, err := indexCache.Get(p, txn)
		if err != nil {
			return err
		}

		O, err := indexCache.Get(o, txn)
		if err != nil {
			return err
		}

		S.Decrement(types.S)
		P.Decrement(types.P)
		O.Decrement(types.O)

		ids := [3][]byte{make([]byte, 8), make([]byte, 8), make([]byte, 8)}
		binary.BigEndian.PutUint64(ids[0], S.Id)
		binary.BigEndian.PutUint64(ids[1], P.Id)
		binary.BigEndian.PutUint64(ids[2], O.Id)

		tripleKey := types.AssembleKey(types.TriplePrefixes[0], ids[0], ids[1], ids[2])
		tripleItem, err := txn.Get(tripleKey)
		if err != nil {
			return err
		}

		sourceList := &types.SourceList{}
		err = tripleItem.Value(func(val []byte) error { return proto.Unmarshal(val, sourceList) })
		if err != nil {
			return err
		}

		n, sources := 0, sourceList.GetSources()
		for _, statement := range sources {
			if statement.Origin == ds.Id && int(statement.Index) == i {
				continue
			} else {
				sources[n] = statement
				n++
			}
		}

		if n == 0 {
			// Delete all the triple keys
			for permutation := types.Permutation(0); permutation < 3; permutation++ {
				a, b, c := types.Major.Permute(permutation, ids)
				key := types.AssembleKey(types.TriplePrefixes[permutation], a, b, c)
				err = txn.Delete(key)
				if err != nil {
					return err
				}
			}
		} else {
			sourceList.Sources = sources[:n]
			val, err := proto.Marshal(sourceList)
			if err != nil {
				return err
			}
			err = txn.Set(tripleKey, val)
			if err != nil {
				return err
			}
		}

		for permutation := types.Permutation(0); permutation < 3; permutation++ {
			majorA, majorB, _ := types.Major.Permute(permutation, ids)
			majorKey := types.AssembleKey(types.MajorPrefixes[permutation], majorA, majorB, nil)
			_, txn, err = types.Decrement(majorKey, 1, txn, db.Badger)
			if err != nil {
				return err
			}

			minorA, minorB, _ := types.Minor.Permute(permutation, ids)
			minorKey := types.AssembleKey(types.MinorPrefixes[permutation], minorA, minorB, nil)
			_, txn, err = types.Decrement(minorKey, 1, txn, db.Badger)
			if err != nil {
				return err
			}
		}
	}

	txn, err = indexCache.Commit(db.Badger, txn)
	if err != nil {
		return err
	}

	return txn.Commit()
}

var prefetchSize = 100

type list struct {
	txn  *badger.Txn
	iter *badger.Iterator
}

func (l *list) Cid() cid.Cid {
	if l.iter.Valid() {
		key := l.iter.Item().KeyCopy(nil)
		c, err := cid.Cast(key[1:])
		if err == nil {
			return c
		}
	}
	return cid.Undef
}

func (l *list) Valid() bool {
	return l.iter.Valid()
}

func (l *list) Close() {
	l.iter.Close()
	l.txn.Discard()
}

func (l *list) Next() {
	l.iter.Next()
}

// List lists the datasets in the database
func (db *DB) List(c cid.Cid) types.List {
	iteratorOptions := badger.IteratorOptions{
		PrefetchValues: true,
		PrefetchSize:   prefetchSize,
		Reverse:        false,
		AllVersions:    false,
		Prefix:         []byte{types.DatasetPrefix},
	}

	txn := db.Badger.NewTransaction(false)
	iter := txn.NewIterator(iteratorOptions)

	var seek []byte
	if c != cid.Undef {
		b := c.Bytes()
		seek = make([]byte, len(b)+1)
		copy(seek[1:], b)
	} else {
		seek = make([]byte, 1)
	}

	seek[0] = types.DatasetPrefix
	iter.Seek(seek)
	return &list{txn, iter}
}

// Log will print the *entire database contents* to log
func (db *DB) Log() {
	values := types.NewValueCache()

	txn := db.Badger.NewTransaction(false)
	defer txn.Discard()

	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	defer iter.Close()

	var i int
	for iter.Seek(nil); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.KeyCopy(nil)
		val, err := item.ValueCopy(nil)
		if err != nil {
			log.Println(err)
			return
		}

		prefix := key[0]
		if bytes.Equal(key, types.DatasetCountKey) {
			count := binary.BigEndian.Uint64(val)
			log.Println("Dataset count", count)
		} else if bytes.Equal(key, types.TripleCountKey) {
			count := binary.BigEndian.Uint64(val)
			log.Println("Triple count", count)
		} else if bytes.Equal(key, types.ValueCountKey) {
			count := binary.BigEndian.Uint64(val)
			log.Println("Value count", count)
		} else if bytes.Equal(key, types.SequenceKey) {
			// Counter!
			log.Printf("Sequence: %02d\n", binary.BigEndian.Uint64(val))
		} else if prefix == types.IndexPrefix {
			// Index key
			index := &types.Index{}
			if err = proto.Unmarshal(val, index); err != nil {
				log.Println(err)
				return
			}
			log.Printf("Index:\n  %s\n  %s\n", string(key[1:]), index.String())
		} else if prefix == types.ValuePrefix {
			// Value key
			value, err := types.GetValue(item)
			if err != nil {
				log.Println(err)
				return
			}
			id := binary.BigEndian.Uint64(key[1:])
			log.Printf("Value: %02d %s\n", id, value.Term(values, db.uri, txn))
		} else if _, has := types.TriplePrefixMap[prefix]; has {
			// Value key
			sourceList := &types.SourceList{}
			proto.Unmarshal(val, sourceList)
			log.Printf("Triple: %s %02d | %02d | %02d :: %s\n",
				string(key[0]),
				binary.BigEndian.Uint64(key[1:9]),
				binary.BigEndian.Uint64(key[9:17]),
				binary.BigEndian.Uint64(key[17:25]),
				types.PrintSources(sourceList.GetSources(), values, db.uri, txn),
			)
		} else if _, has := types.MinorPrefixMap[prefix]; has {
			// Minor key
			log.Printf("Minor index: %s %02d | %02d :: %02d\n",
				string(key[0]),
				binary.BigEndian.Uint64(key[1:9]),
				binary.BigEndian.Uint64(key[9:17]),
				binary.BigEndian.Uint64(val),
			)
		} else if _, has := types.MajorPrefixMap[prefix]; has {
			// Major key
			log.Printf("Major index: %s %02d | %02d :: %02d\n",
				string(key[0]),
				binary.BigEndian.Uint64(key[1:9]),
				binary.BigEndian.Uint64(key[9:17]),
				binary.BigEndian.Uint64(val),
			)
		} else if prefix == types.DatasetPrefix {
			c, err := cid.Cast(key[1:])
			if err != nil {
				log.Println(err)
				return
			}
			log.Printf("Dataset: <%s>\n", db.uri.String(c, ""))
		}
		i++
	}
	log.Printf("Printed %02d database entries\n", i)
	return
}
