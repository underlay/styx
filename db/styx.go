package db

import (
	"fmt"

	badger "github.com/dgraph-io/badger/v2"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	query "github.com/underlay/styx/query"
	types "github.com/underlay/styx/types"
)

// DB is the general styx database wrapper
type DB struct {
	badger *badger.DB
	Loader ld.DocumentLoader
}

// Pinner is anything that takes a string and returns a CID
type Pinner = func(normalized string) (cid.Cid, error)

// Close the Shit
func (db *DB) Close() error {
	return db.badger.Close()
}

// OpenDB opens a styx database
func OpenDB(path string) (*DB, error) {
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &DB{
		badger: db,
	}, nil
}

// Ingest a document
func (db *DB) Ingest(cid cid.Cid, graph string, quads []*ld.Quad) error {
	return db.badger.Update(func(txn *badger.Txn) error {
		return insert(cid, graph, quads, txn)
	})
}

// Query the database
func (db *DB) Query(graph string, quads []*ld.Quad, result chan []*ld.Quad) error {
	defer func() { result <- nil }()
	return db.badger.View(func(txn *badger.Txn) error {
		assignmentMap, err := query.SolveGraph(graph, quads, txn)
		fmt.Println("got things", assignmentMap, err)
		if err != nil {
			return err
		}

		values := map[[8]byte]*types.Value{}
		for _, quad := range quads {
			quad.Subject, err = setValues(quad.Subject, assignmentMap, values, txn)
			if err != nil {
				return err
			}

			quad.Predicate, err = setValues(quad.Predicate, assignmentMap, values, txn)
			if err != nil {
				return err
			}

			quad.Object, err = setValues(quad.Object, assignmentMap, values, txn)
			if err != nil {
				return err
			}
		}

		result <- quads
		return nil
	})
}

func setValues(node ld.Node, assignmentMap *query.AssignmentMap, values map[[8]byte]*types.Value, txn *badger.Txn) (ld.Node, error) {
	blank, isBlank := node.(*ld.BlankNode)
	if !isBlank {
		return node, nil
	}

	assignment := assignmentMap.Index[blank.Attribute]
	if value, has := values[assignment.Value]; has {
		return valueToNode(value)
	}

	key := make([]byte, 9)
	key[0] = types.ValuePrefix
	copy(key[1:9], assignment.Value[:])
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

	values[assignment.Value] = value
	return valueToNode(value)
}
