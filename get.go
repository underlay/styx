package styx

import (
	badger "github.com/dgraph-io/badger/v2"
	ld "github.com/piprate/json-gold/ld"
)

// Get a dataset from the database
func (db *Styx) Get(uri string) ([]*ld.Quad, error) {
	txn := db.badger.NewTransaction(true)
	defer func() { txn.Discard() }()

	key := make([]byte, 1+len(uri))
	key[0] = DatasetPrefix
	copy(key[1:], uri)

	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	quads, err := db.get(item)
	if err != nil {
		return nil, err
	}

	values := newValueCache()
	origin, err := values.GetID(uri, txn)
	dataset := make([]*ld.Quad, len(quads))
	for i, quad := range quads {
		dataset[i] = &ld.Quad{
			Subject:   quad[0].Node(origin, values, txn),
			Predicate: quad[1].Node(origin, values, txn),
			Object:    quad[2].Node(origin, values, txn),
			Graph:     quad[3].Node(origin, values, txn),
		}
	}

	return dataset, nil
}

func (db *Styx) get(item *badger.Item) (quads [][4]Value, err error) {
	quads = make([][4]Value, 0)
	err = item.Value(func(val []byte) error {
		for i := 0; i < len(val); {
			quad := [4]Value{}
			for j := 0; j < 4; j++ {
				v, l := readValue(val[i:])
				quad[j] = v
				i += l
			}
			quads = append(quads, quad)
		}
		return nil
	})

	return
}
