package styx

import (
	badger "github.com/dgraph-io/badger/v2"
)

// A List is an iterator over dataset URIs
type List interface {
	URI() string
	Next()
	Valid() bool
	Close()
}

const prefetchSize = 100

type list struct {
	txn  *badger.Txn
	iter *badger.Iterator
}

func (l *list) URI() string {
	if l.iter.Valid() {
		key := l.iter.Item().KeyCopy(nil)
		return string(key[1:])
	}
	return ""
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
func (db *Styx) List(uri string) List {
	iteratorOptions := badger.IteratorOptions{
		PrefetchValues: false,
		PrefetchSize:   prefetchSize,
		Reverse:        false,
		AllVersions:    false,
		Prefix:         []byte{DatasetPrefix},
	}

	txn := db.Badger.NewTransaction(false)
	iter := txn.NewIterator(iteratorOptions)

	var seek []byte
	if uri == "" {
		seek = make([]byte, 1)
	} else {
		seek = make([]byte, len(uri)+1)
		copy(seek[1:], string(uri))
	}

	seek[0] = DatasetPrefix
	iter.Seek(seek)
	return &list{txn, iter}
}
