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
func IngestJSONLd(db types.Styx, api core.CoreAPI, doc interface{}) error {
	proc := ld.NewJsonLdProcessor()
	opts := ld.NewJsonLdOptions("")
	opts.DocumentLoader = ld.NewDwebDocumentLoader(api)
	opts.Algorithm = types.Algorithm
	d, err := proc.ToRDF(doc, opts)
	if err != nil {
		return err
	}

	opts.Format = types.Format
	na := ld.NewNormalisationAlgorithm(opts.Algorithm)
	na.Normalize(d.(*ld.RDFDataset))
	nquads, err := na.Format(opts)
	if err != nil {
		return err
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
		return err
	}

	return db.Insert(resolved.Cid(), na.Quads())
}

// Query satisfies the Styx interface
func (db *DB) Query(pattern []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (types.Cursor, error) {
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
	// TODO: implement...
	return
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
