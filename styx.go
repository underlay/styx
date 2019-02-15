package main

import (
	"encoding/binary"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"
)

func marshalReferenceNode(node ld.Node, assignmentMap *AssignmentMap, txn *badger.Txn) (string, error) {
	blank, isBlank := node.(*ld.BlankNode)
	if !isBlank {
		return marshalNode(nil, node), nil
	}
	valueID := assignmentMap.Index[blank.Attribute].Value
	valueKey := make([]byte, 9)
	valueKey[0] = ValuePrefix
	binary.BigEndian.PutUint64(valueKey[1:9], valueID)
	item, err := txn.Get(valueKey)
	if err != nil {
		return "", err
	}
	buffer, err := item.ValueCopy(nil)
	if err != nil {
		return "", err
	}
	value := &Value{}
	err = proto.Unmarshal(buffer, value)
	if err != nil {
		return "", err
	}
	return marshalValue(value)
}

// Query the database
func Query(query interface{}, callback func(result interface{}) error, db *badger.DB, sh *ipfs.Shell) error {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(sh)
	options.ProcessingMode = ld.JsonLd_1_1
	options.UseNativeTypes = true
	options.Explicit = true

	if asMap, isMap := query.(map[string]interface{}); isMap {
		_, hasGraph := asMap["@graph"]
		options.OmitGraph = !hasGraph
	}

	// Convert to RDF
	rdf, err := proc.Normalize(query, options)
	if err != nil {
		return err
	}

	dataset := rdf.(*ld.RDFDataset)
	printDataset(dataset)
	return db.View(func(txn *badger.Txn) error {
		index, err := solveDataset(dataset, txn)
		if err != nil {
			return err
		}

		var result string
		for _, quad := range dataset.Graphs[DefaultGraph] {
			subject, err := marshalReferenceNode(quad.Subject, index, txn)
			if err != nil {
				return err
			}
			predicate, err := marshalReferenceNode(quad.Predicate, index, txn)
			if err != nil {
				return err
			}
			object, err := marshalReferenceNode(quad.Object, index, txn)
			result += subject + " " + predicate + " " + object + " .\n"
		}

		fmt.Println(result)
		document, err := proc.FromRDF(result, options)
		if err != nil {
			return err
		}

		framed, err := proc.Frame(document, query, options)
		if err != nil {
			return err
		}

		return callback(framed)
	})
}

// Ingest a document
func Ingest(doc interface{}, db *badger.DB, sh *ipfs.Shell) (string, error) {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(sh)

	// Convert to RDF
	rdf, err := proc.ToRDF(doc, options)
	if err != nil {
		return "", err
	}

	dataset := rdf.(*ld.RDFDataset)

	// Normalize and add to IFPS
	options.Format = Format
	options.Algorithm = Algorithm
	api := ld.NewJsonLdApi()
	normalized, err := api.Normalize(dataset, options)
	if err != nil {
		return "", err
	}

	fmt.Println("normalized")
	fmt.Println(normalized)

	reader := strings.NewReader(normalized.(string))
	hash, err := sh.Add(reader)
	if err != nil {
		return hash, err
	}

	return hash, db.Update(func(txn *badger.Txn) error {
		return insert(hash, dataset, txn)
	})
}
