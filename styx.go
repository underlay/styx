package styx

import (
	"fmt"
	"strings"

	ipfs "github.com/ipfs/go-ipfs-api"

	"github.com/dgraph-io/badger"
	"github.com/piprate/json-gold/ld"
)

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

func printDataset(dataset *ld.RDFDataset) {
	for graph, quads := range dataset.Graphs {
		fmt.Printf("%s:\n", graph)
		for i, quad := range quads {
			fmt.Printf("%2d: %s %s %s\n", i, quad.Subject.GetValue(), quad.Predicate.GetValue(), quad.Object.GetValue())
		}
	}
}
