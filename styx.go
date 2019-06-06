package main

import (
	"strings"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"

	"./loader"
	"./query"
	"./types"
)

// Ingest a document
func Ingest(doc interface{}, db *badger.DB, sh *ipfs.Shell) (string, error) {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = loader.NewDwebDocumentLoader(sh)

	// Convert to RDF
	rdf, err := proc.ToRDF(doc, options)
	if err != nil {
		return "", err
	}

	dataset := rdf.(*ld.RDFDataset)

	// Normalize and add to IFPS
	options.Format = types.Format
	options.Algorithm = types.Algorithm
	api := ld.NewJsonLdApi()
	normalized, err := api.Normalize(dataset, options)
	if err != nil {
		return "", err
	}

	reader := strings.NewReader(normalized.(string))
	hash, err := sh.Add(reader)
	if err != nil {
		return hash, err
	}

	return hash, db.Update(func(txn *badger.Txn) error {
		return insert(hash, dataset, txn)
	})
}

// Query the database
func Query(q interface{}, callback func(result interface{}) error, db *badger.DB, sh *ipfs.Shell) error {
	proc := ld.NewJsonLdProcessor()
	api := ld.NewJsonLdApi()

	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = loader.NewDwebDocumentLoader(sh)
	options.ProcessingMode = ld.JsonLd_1_1
	options.UseNativeTypes = true
	options.Explicit = true

	if asMap, isMap := q.(map[string]interface{}); isMap {
		_, hasGraph := asMap["@graph"]
		options.OmitGraph = !hasGraph
	}

	// Convert to RDF
	rdf, err := proc.Normalize(q, options)
	if err != nil {
		return err
	}

	dataset := rdf.(*ld.RDFDataset)

	return db.View(func(txn *badger.Txn) error {
		assignmentMap, err := query.SolveDataset(dataset, txn)
		if err != nil {
			return err
		}

		values := map[[8]byte]*types.Value{}
		for _, quad := range dataset.Graphs[types.DefaultGraph] {
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

		doc, err := api.FromRDF(dataset, options)
		if err != nil {
			return err
		}

		framed, err := proc.Frame(doc, q, options)
		if err != nil {
			return err
		}

		return callback(framed)
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
