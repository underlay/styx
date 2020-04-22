package styx

import (
	"errors"
	"sort"
	"strings"

	badger "github.com/dgraph-io/badger/v2"
)

// QuadStore is an interface for things that can persist datasets
type QuadStore interface {
	Set(id ID, quads [][4]ID) error
	Get(id ID) ([][4]ID, error)
	Delete(id ID) error
	List(id ID) interface {
		Next() (id ID, valid bool)
		Close()
	}
}

type emtpyList struct{}
type emptyStore struct{}

func MakeEmpyStore() QuadStore { return emptyStore{} }

func (el emtpyList) Close()                                { return }
func (el emtpyList) Next() (id ID, valid bool)             { return }
func (es emptyStore) Set(id ID, quads [][4]ID) (err error) { return }
func (es emptyStore) Get(id ID) (quads [][4]ID, err error) { return }
func (es emptyStore) Delete(id ID) (err error)             { return }
func (es emptyStore) List(id ID) interface {
	Next() (id ID, valid bool)
	Close()
} {
	return emtpyList{}
}

type memoryStore struct {
	datasets map[string][][4]ID
	values   []string
}

func MakeMemoryStore() QuadStore {
	return &memoryStore{
		datasets: map[string][][4]ID{},
		values:   []string{},
	}
}

func (m *memoryStore) Set(id ID, quads [][4]ID) error {
	value := string(id)
	m.datasets[value] = quads
	i := sort.SearchStrings(m.values, value)
	m.values = append(m.values, "")
	copy(m.values[i+1:], m.values[i:])
	m.values[i] = value
	return nil
}

func (m *memoryStore) Get(id ID) ([][4]ID, error) {
	value := string(id)
	dataset, has := m.datasets[value]
	if has {
		return dataset, nil
	}

	return nil, ErrNotFound
}

func (m *memoryStore) Delete(id ID) error {
	value := string(id)
	_, has := m.datasets[value]
	if !has {
		return ErrNotFound
	}

	delete(m.datasets, value)
	i := sort.SearchStrings(m.values, value)
	m.values = append(m.values[:i], m.values[i+1:]...)
	return nil
}

type memoryList struct {
	int
	*memoryStore
}

func (ml *memoryList) Close() {}
func (ml *memoryList) Next() (id ID, valid bool) {
	if ml.int < len(ml.memoryStore.values) {
		id, valid = ID(ml.memoryStore.values[ml.int]), true
		ml.int++
	}
	return
}

func (m *memoryStore) List(id ID) interface {
	Next() (id ID, valid bool)
	Close()
} {
	value := string(id)
	i := sort.SearchStrings(m.values, value)
	return &memoryList{i, m}
}

type badgerStore struct{ Badger *badger.DB }

// MakeBadgerStore creates new badger quad store
func MakeBadgerStore(db *badger.DB) QuadStore { return &badgerStore{Badger: db} }

func (b *badgerStore) Get(id ID) ([][4]ID, error) {
	txn := b.Badger.NewTransaction(false)
	defer func() { txn.Discard() }()

	key := assembleKey(DatasetPrefix, false, id)
	item, err := txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	return getQuads(item)
}

// ErrParseQuads indicates that a TSV of quads could not be parsed
var ErrParseQuads = errors.New("Error parsing quads from Badger datastore")

func getQuads(item *badger.Item) (quads [][4]ID, err error) {
	err = item.Value(func(val []byte) error {
		lines := strings.Split(string(val), "\n")
		if len(lines) < 2 {
			return ErrParseQuads
		}

		quads = make([][4]ID, len(lines))
		for i, line := range lines {
			terms := strings.Split(line, "\t")
			if len(terms) != 4 {
				return ErrParseQuads
			}
			quads[i] = [4]ID{}
			for j, id := range terms {
				quads[i][j] = ID(id)
			}
		}

		return nil
	})

	return
}

func (b *badgerStore) Delete(id ID) (err error) {
	key := assembleKey(DatasetPrefix, false, id)
	return b.Badger.Update(func(txn *badger.Txn) error { return txn.Delete(key) })
}

func (b *badgerStore) Set(id ID, quads [][4]ID) error {
	lines := make([]string, len(quads))
	for i, quad := range quads {
		line := make([]string, 4)
		for j, term := range quad {
			line[j] = string(term)
		}
		lines[i] = strings.Join(line, "\t")
	}
	val := strings.Join(lines, "\n")
	key := assembleKey(DatasetPrefix, false, id)
	return b.Badger.Update(func(txn *badger.Txn) error { return txn.Set(key, []byte(val)) })
}

type badgerList struct {
	txn  *badger.Txn
	iter *badger.Iterator
}

func (bl *badgerList) Close() { bl.iter.Close(); bl.txn.Discard() }
func (bl *badgerList) Next() (id ID, valid bool) {
	if bl.iter.Valid() {
		key := bl.iter.Item().KeyCopy(nil)
		id, valid = ID(key[1:]), true
		bl.iter.Next()
	}
	return
}

func (b *badgerStore) List(id ID) interface {
	Next() (id ID, valid bool)
	Close()
} {
	key := assembleKey(DatasetPrefix, false, id)
	txn := b.Badger.NewTransaction(false)
	iter := txn.NewIterator(badger.IteratorOptions{
		PrefetchValues: false,
		Prefix:         []byte{DatasetPrefix},
	})
	iter.Seek(key)
	return &badgerList{txn, iter}
}
