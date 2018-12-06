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

const initialCounter uint64 = 1

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
	fmt.Println("normalizedd")
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
