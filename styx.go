package styx

import (
	"fmt"
	"strings"

	ipfs "github.com/ipfs/go-ipfs-api"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

// algorithm can be URDNA2015 or URGNA2012
const algorithm = "URDNA2015"

// format has to be application/nquads
const format = "application/nquads"

// InitialCounter is the first uint64 value we start counting from.
// Let's set it to 1 just in case we want to ever use 0 for something special.
const InitialCounter uint64 = 1

func ingest(doc interface{}, db *badger.DB, shell *ipfs.Shell) error {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(nil)

	// Convert to RDF
	rdf, err := proc.ToRDF(doc, options)
	if err != nil {
		return err
	}
	dataset := rdf.(*ld.RDFDataset)

	// Normalize and add to IFPS
	options.Format = format
	options.Algorithm = algorithm
	api := ld.NewJsonLdApi()
	normalized, err := api.Normalize(dataset, options)
	fmt.Println("normalized")
	fmt.Println(normalized)
	if err != nil {
		return err
	}

	reader := strings.NewReader(normalized.(string))
	cid, err := shell.Add(reader)
	if err != nil {
		return err
	}
	return db.Update(func(txn *badger.Txn) error {
		return insert(cid, dataset, txn)
	})
}

/*
???
*/
