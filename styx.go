package styx

import (
	"fmt"
	"strings"

	ipfs "github.com/ipfs/go-ipfs-api"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

// Algorithm has to be URDNA2015
const Algorithm = "URDNA2015"

// Format has to be application/nquads
const Format = "application/nquads"

// InitialCounter is the first uint64 value we start counting from.
// Let's set it to 1 just in case we want to ever use 0 for something special.
const InitialCounter uint64 = 1

// ConstantPermutation is the value we give to all-constant references.
// We don't even use them for now.
// Let's make it 7 to be really weird.
const ConstantPermutation uint8 = 7

func ingest(doc interface{}, db *badger.DB, sh *ipfs.Shell) error {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(sh)

	// Convert to normnalized RDF
	rdf, err := proc.Normalize(doc, options)
	if err != nil {
		return err
	}

	dataset := rdf.(*ld.RDFDataset)

	// Normalize and add to IFPS
	options.Format = Format
	options.Algorithm = Algorithm
	api := ld.NewJsonLdApi()
	normalized, err := api.Normalize(dataset, options)
	if err != nil {
		return err
	}

	fmt.Println("normalized")
	fmt.Println(normalized)

	reader := strings.NewReader(normalized.(string))
	cid, err := sh.Add(reader)
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
