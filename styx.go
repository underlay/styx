package styx

import (
	"bytes"
	"encoding/binary"
	"log"
	"net/url"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
	"github.com/google/uuid"
	ld "github.com/piprate/json-gold/ld"
	rdf "github.com/underlay/go-rdfjs"
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

type nilTagScheme struct{}

func (nts nilTagScheme) Test(uri string) bool                    { return false }
func (nts nilTagScheme) Parse(uri string) (tag, fragment string) { return }

type prefixTagScheme string

// NewPrefixTagScheme creates a tag scheme that tests for the given prefix
func NewPrefixTagScheme(prefix string) TagScheme { return prefixTagScheme(prefix) }

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
	Badger *badger.DB
	// Sequence *badger.Sequence
	Config *Config
}

// Config contains the initialization options passed to Styx
type Config struct {
	Path       string
	TagScheme  TagScheme
	Canonize   bool
	Dictionary DictionaryFactory
	QuadStore  QuadStore
}

// Close the database
func (s *Store) Close() (err error) {
	if s == nil {
		return
	}

	if s.Config.Dictionary != nil {
		err = s.Config.Dictionary.Close()
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
func NewStore(config *Config) (*Store, error) {
	if config == nil {
		config = &Config{}
	}

	opts := badger.DefaultOptions(config.Path)
	if config.Path == "" {
		opts = opts.WithInMemory(true)
		log.Println("Opening in-memory badger database")
	} else {
		log.Println("Opening badger database at", config.Path)
	}

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	if config.TagScheme == nil {
		config.TagScheme = nilTagScheme{}
	}

	if config.Dictionary == nil {
		config.Dictionary = StringDictionary
	}

	if config.QuadStore == nil {
		config.QuadStore = MakeBadgerStore(db)
	}

	config.Dictionary.Init(db, config.TagScheme)

	return &Store{
		Config: config,
		Badger: db,
	}, nil
}

// QueryJSONLD exposes a JSON-LD query interface
func (s *Store) QueryJSONLD(query interface{}) (*Iterator, error) {
	opts := ld.NewJsonLdOptions("")
	opts.ProduceGeneralizedRdf = true
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	base := "urn:uuid:" + id.String() + "?"
	opts.ExpandContext = map[string]interface{}{"?": base}
	dataset, err := getDataset(query, opts)
	if err != nil {
		return nil, err
	}
	quads := fromLdDataset(dataset, base)
	return s.Query(quads, nil, nil)
}

// Query satisfies the Styx interface
func (s *Store) Query(pattern []*rdf.Quad, domain []rdf.Term, index []rdf.Term) (*Iterator, error) {
	txn := s.Badger.NewTransaction(false)
	dictionary := s.Config.Dictionary.Open(false)
	iter, err := newIterator(pattern, domain, index, s.Config.TagScheme, txn, dictionary)
	if err != nil {
		iter.Close()
	}

	if err == badger.ErrKeyNotFound || err == ErrEmptyInterset {
		err = nil
		iter.top = true
	}

	return iter, err
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
			log.Printf("Dataset: %s\n", string(key[1:]))
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
