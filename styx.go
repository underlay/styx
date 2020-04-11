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
const tmpPath = "/tmp/styx"

// SequenceBandwidth sets the lease block size of the ID counter
const SequenceBandwidth = 512

// A TagScheme is an interface for testing whether a given URI is a dataset URI or not
type TagScheme interface {
	Test(uri string) bool
	Parse(uri string) (tag string, fragment string)
}

type prefixTagScheme string

// NewPrefixTagScheme creates a tag scheme that tests for the given prefix
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

// A Store is a database instance
type Store struct {
	Badger   *badger.DB
	Sequence *badger.Sequence
	Options  *Options
}

// Options are the initialization options passed to Styx
type Options struct {
	Path      string
	TagScheme TagScheme
	Canonize  bool
}

// Close the database
func (s *Store) Close() (err error) {
	if s == nil {
		return
	}
	if s.Sequence != nil {
		err = s.Sequence.Release()
		if err != nil {
			return
		}
	}
	if s.Badger != nil {
		err = s.Badger.Close()
		if err != nil {
			return
		}
	}
	return
}

// NewStore opens a styx database
func NewStore(options *Options) (*Store, error) {
	if options == nil {
		options = &Options{}
	}

	opts := badger.DefaultOptions(options.Path)
	if options.Path == "" {
		opts = opts.WithInMemory(true)
		log.Println("Opening in-memory badger database")
	} else {
		log.Println("Opening badger database at", options.Path)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	txn := db.NewTransaction(true)
	_, err = txn.Get(SequenceKey)
	if err == badger.ErrKeyNotFound {
		// Yay! Now we have to write an initial one
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, 128)
		err = txn.Set(SequenceKey, val)
		if err != nil {
			txn.Discard()
			return nil, err
		}
		err = txn.Commit()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	seq, err := db.GetSequence(SequenceKey, SequenceBandwidth)
	if err != nil {
		return nil, err
	}

	return &Store{
		Options:  options,
		Badger:   db,
		Sequence: seq,
	}, nil
}

// QueryJSONLD exposes a JSON-LD query interface
func (s *Store) QueryJSONLD(query interface{}) (*Iterator, error) {
	opts := ld.NewJsonLdOptions("")
	opts.ProduceGeneralizedRdf = true
	dataset, err := getDataset(query, opts)
	if err != nil {
		return nil, err
	}
	return s.Query(dataset.Graphs["@default"], nil, nil)
}

// Query satisfies the Styx interface
func (s *Store) Query(pattern []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (*Iterator, error) {
	txn := s.Badger.NewTransaction(false)
	g, err := MakeConstraintGraph(pattern, domain, index, s.Options.TagScheme, txn)
	if err != nil {
		g.Close()
	}

	if err == badger.ErrKeyNotFound {
		err = ErrEndOfSolutions
	}

	return g, err
}

// Log will print the *entire database contents* to log
func (s *Store) Log() {
	txn := s.Badger.NewTransaction(false)
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
