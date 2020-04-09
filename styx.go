package styx

import (
	"bytes"
	"encoding/binary"
	"log"
	"net/url"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// DefaultPath is the default path for the Badger database
const DefaultPath = "/tmp/styx"

// SequenceBandwidth sets the lease block size of the ID counter
const SequenceBandwidth = 512

type prefixTagScheme string

func NewPrefixTagScheme(prefix string) TagScheme {
	return prefixTagScheme(prefix)
}

func (pts prefixTagScheme) Test(uri string) bool {
	return strings.Index(uri, string(pts)) == 0 && strings.Index(uri, "#") >= len(pts)
}

func (pts prefixTagScheme) Parse(uri string) (tag, fragment string) {
	u, err := url.Parse(uri)
	if err == nil {
		fragment = u.Fragment
		tag = strings.TrimSuffix(uri, "#"+fragment)
	}
	return
}

// Styx is a stupid interface
type Styx interface {
	Query(query []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (Cursor, error)
	Set(uri string, dataset []*ld.Quad) error
	Get(uri string) ([]*ld.Quad, error)
	Delete(uri string) error
	List(uri string) List
	Close() error
	Log()
}

// Styx is the general styx database wrapper
type styx struct {
	tag      TagScheme
	badger   *badger.DB
	sequence *badger.Sequence
}

// Close the database
func (db *styx) Close() (err error) {
	if db == nil {
		return
	}
	if db.sequence != nil {
		err = db.sequence.Release()
		if err != nil {
			return
		}
	}
	if db.badger != nil {
		err = db.badger.Close()
		if err != nil {
			return
		}
	}
	return
}

// OpenDB opens a styx database
func OpenDB(path string, tag TagScheme) (*styx, error) {
	opts := badger.DefaultOptions(path)
	if path == "" {
		opts = opts.WithInMemory(path == "")
		log.Println("Opening in-memory badger database")
	} else {
		log.Println("Opening badger database at", path)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	txn := db.NewTransaction(true)
	defer txn.Discard()
	_, err = txn.Get(SequenceKey)
	if err == badger.ErrKeyNotFound {
		// Yay! Now we have to write an initial one
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, 128)
		err = txn.Set(SequenceKey, val)
		if err != nil {
			return nil, err
		}
		err = txn.Commit()
	}

	txn.Discard() // ??
	if err != nil {
		return nil, err
	}

	seq, err := db.GetSequence(SequenceKey, SequenceBandwidth)
	if err != nil {
		return nil, err
	}

	return &styx{
		tag:      tag,
		badger:   db,
		sequence: seq,
	}, nil
}

// IngestJSONLd takes a JSON-LD document and ingests it.
// This is mostly a convenience method for testing;
// actual messages should get handled at HandleMessage.
func IngestJSONLd(db *styx, uri string, doc interface{}, canonize bool) ([]*ld.Quad, error) {
	proc := ld.NewJsonLdProcessor()
	opts := ld.NewJsonLdOptions("")
	opts.Algorithm = Algorithm
	d, err := proc.ToRDF(doc, opts)
	if err != nil {
		return nil, err
	}

	dataset := d.(*ld.RDFDataset)
	var quads []*ld.Quad

	if canonize {
		opts.Format = Format
		na := ld.NewNormalisationAlgorithm(opts.Algorithm)
		na.Normalize(dataset)
		quads = na.Quads()
	} else {
		quads = make([]*ld.Quad, 0)
		for _, graph := range dataset.Graphs {
			quads = append(quads, graph...)
		}
	}

	return quads, db.Set(uri, quads)
}

// Query satisfies the Styx interface
func (db *styx) Query(pattern []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (Cursor, error) {
	txn := db.badger.NewTransaction(false)
	g, err := MakeConstraintGraph(pattern, domain, index, db.tag, txn)
	if err != nil {
		g.Close()
	}

	if err == badger.ErrKeyNotFound {
		err = ErrEndOfSolutions
	}

	return g, err
}

// Log will print the *entire database contents* to log
func (db *styx) Log() {
	txn := db.badger.NewTransaction(false)
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
		if bytes.Equal(key, SequenceKey) {
			log.Printf("Sequence: %02d\n", binary.BigEndian.Uint64(val))
		} else if prefix == ValueToIDPrefix {
			// Value key
			value := string(key[1:])
			if err != nil {
				log.Println(err)
				return
			}
			log.Printf("Value to ID: %s -> %s\n", value, string(val))
		} else if prefix == IDToValuePrefix {
			// Value key
			id := iri(key[1:])
			if err != nil {
				log.Println(err)
				return
			}
			log.Printf("ID to Value: %s <- %s\n", id, string(val))
		} else if 'a' <= prefix && prefix <= 'c' {
			// Ternary key
			log.Println(
				"Ternary entry:",
				string(prefix),
				strings.Replace(string(key[1:]), "\t", " ", -1),
				"->",
				"|"+strings.Replace(strings.Replace(string(val), "\t", " ", -1), "\n", "|", -1),
			)
		} else if 'i' <= prefix && prefix <= 'n' {
			// Binary key
			log.Println(
				"Binary entry:",
				string(prefix),
				strings.Replace(string(key[1:]), "\t", " ", -1),
				"->",
				binary.BigEndian.Uint32(val),
			)
		} else if prefix == DatasetPrefix {
			log.Printf("Dataset: <%s>\n", string(key[1:]))
		} else if prefix == UnaryPrefix {
			if len(val) != 24 {
				log.Println("Unexpected index value", val)
				return
			}

			index := &[6]uint32{}
			for i := 0; i < 6; i++ {
				index[i] = binary.BigEndian.Uint32(val[i*4 : (i+1)*4])
			}
			log.Println(
				"Unary entry:",
				string(prefix),
				string(key[1:]),
				"->",
				*index,
			)
		} else {
			// Some other key...
		}
		i++
	}
	log.Printf("Printed %02d database entries\n", i)
	return
}
