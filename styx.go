package styx

import (
	"fmt"
	"log"
	"strings"

	badger "github.com/dgraph-io/badger"
	cid "github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"
)

func (source *Source) toCompactString() string {
	c, err := cid.Parse(source.Cid)
	if err != nil {
		log.Fatalln(err)
	}
	return fmt.Sprintf("%s#%s[%d]", c.String(), source.Graph, source.Index)
}

func (sourceList *SourceList) toCompactString() string {
	s := "["
	for i, source := range sourceList.Sources {
		if i > 0 {
			s += ", "
		}
		s += source.toCompactString()
	}
	return s + "]"
}

func ingest(doc interface{}, db *badger.DB, sh *ipfs.Shell) (string, error) {
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

func printDataset(dataset *ld.RDFDataset) {
	for graph, quads := range dataset.Graphs {
		fmt.Printf("%s:\n", graph)
		for i, quad := range quads {
			fmt.Printf("%2d: %s %s %s\n", i, quad.Subject.GetValue(), quad.Predicate.GetValue(), quad.Object.GetValue())
		}
	}
}
