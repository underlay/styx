package main

import (
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger"
	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"
)

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
			result += string(marshalReferenceNode(quad.Subject, index))
			result += " "
			result += string(marshalReferenceNode(quad.Predicate, index))
			result += " "
			result += string(marshalReferenceNode(quad.Object, index))
			result += " .\n"
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
	cid, err := sh.Add(reader)
	if err != nil {
		return cid, err
	}

	return cid, db.Update(func(txn *badger.Txn) error {
		return insert(cid, dataset, txn)
	})
}
