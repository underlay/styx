package db

import (
	"encoding/binary"
	"fmt"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	query "github.com/underlay/styx/query"
	types "github.com/underlay/styx/types"
)

// DB is the general styx database wrapper
type DB struct {
	Badger *badger.DB
	Loader ld.DocumentLoader
}

// Pinner is anything that takes a string and returns a CID
type Pinner = func(nquads []byte) (cid.Cid, error)

// Close the Shit
func (db *DB) Close() error {
	return db.Badger.Close()
}

// OpenDB opens a styx database
func OpenDB(path string) (*DB, error) {
	opts := badger.DefaultOptions(path)

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &DB{
		Badger: db,
	}, nil
}

// Ingest a document
func (db *DB) Ingest(cid cid.Cid, graph string, quads []*ld.Quad) error {
	if graph == "@default" {
		graph = ""
	}

	return db.Badger.Update(func(txn *badger.Txn) error {
		return insert(cid, graph, quads, txn)
	})
}

// Query the database
func (db *DB) Query(quads []*ld.Quad, result chan []*ld.Quad) error {
	fmt.Println("querying!")
	defer func() { result <- nil }()
	err := db.Badger.View(func(txn *badger.Txn) (err error) {
		fmt.Println("got query view")
		var g *query.ConstraintGraph
		if g, err = query.MakeConstraintGraph(quads, txn); err != nil {
			return
		}

		fmt.Println("got constraing graph")
		fmt.Println(g)

		if err = g.Solve(txn); err != nil {
			return
		}

		fmt.Println("solved query")

		values := map[uint64]*types.Value{}
		for _, quad := range quads {
			quad.Subject, err = setValues(quad.Subject, g, values, txn)
			if err != nil {
				return err
			}

			quad.Predicate, err = setValues(quad.Predicate, g, values, txn)
			if err != nil {
				return err
			}

			quad.Object, err = setValues(quad.Object, g, values, txn)
			if err != nil {
				return err
			}
		}

		fmt.Println("sending results!")
		result <- quads
		return nil
	})
	fmt.Println("returning from outer function thing")
	return err
}

func setValues(node ld.Node, g *query.ConstraintGraph, values map[uint64]*types.Value, txn *badger.Txn) (ld.Node, error) {
	blank, isBlank := node.(*ld.BlankNode)
	if !isBlank {
		return node, nil
	}

	u := g.Index[blank.Attribute]
	id := binary.BigEndian.Uint64(u.Value)

	if value, has := values[id]; has {
		return value, nil
	}

	key := make([]byte, 9)
	key[0] = types.ValuePrefix
	copy(key[1:9], u.Value)
	item, err := txn.Get(key)
	if err != nil {
		return nil, err
	}

	buf, err := item.ValueCopy(nil)
	if err != nil {
		return nil, err
	}

	value := &types.Value{}
	err = proto.Unmarshal(buf, value)
	if err != nil {
		return nil, err
	}

	values[id] = value
	return value, nil
}
