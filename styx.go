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

func sourcesToString(sources []*Source) string {
	s := "["
	for i, source := range sources {
		if i > 0 {
			s += ", "
		}
		s += source.toCompactString()
	}
	return s + "]"
}

func printCodexMap(c *CodexMap) {
	fmt.Println("----- Codex Map -----")
	for _, id := range c.Slice {
		fmt.Printf("---- %s ----\n%s\n", id, c.Index[id].String())
	}
	fmt.Println("----- End of Codex Map -----")
}

func query(doc interface{}, db *badger.DB, sh *ipfs.Shell) error {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(sh)

	// Convert to RDF
	rdf, err := proc.Normalize(doc, options)
	if err != nil {
		return err
	}

	dataset := rdf.(*ld.RDFDataset)
	printDataset(dataset)
	return db.View(func(txn *badger.Txn) error {
		return solveDataset(dataset, txn)
	})
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

func printAssignments(slice []string, index map[string]*Assignment) {
	fmt.Println("printing assignments", slice)
	for _, id := range slice {
		a := index[id]
		fmt.Printf("id: %s\n", id)
		fmt.Println(a.String())
	}
}
