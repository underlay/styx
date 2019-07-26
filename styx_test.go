package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	ld "github.com/piprate/json-gold/ld"

	styx "github.com/underlay/styx/db"
	loader "github.com/underlay/styx/loader"
)

var sampleData = []byte(`{
	"@context": {
		"@vocab": "http://schema.org/",
		"xsd": "http://www.w3.org/2001/XMLSchema#",
		"prov": "http://www.w3.org/ns/prov#",
		"prov:generatedAtTime": {
			"@type": "xsd:dateTime"
		},
		"birthDate": {
			"@type": "xsd:date"
		}
	},
	"prov:generatedAtTime": "2019-07-24T16:46:05.751Z",
	"@graph": {
		"@type": "Person",
		"name": "John Doe",
		"birthDate": "1996-02-02",
		"knows": {
			"@id": "http://people.com/jane",
			"@type": "Person",
			"name": "Jane Doe",
			"birthDate": "1995-01-01"
		}
	}
}`)

var sampleQuery = []byte(`{
	"@context": {
		"@vocab": "http://schema.org/"
	},
	"@type": "Person",
	"birthDate": { },
	"knows": {
		"name": "Jane Doe"
	}
}`)

func TestIPFSDocumentLoader(t *testing.T) {
	data := []byte(`{
		"@context": { "@vocab": "http://schema.org/" },
		"name": "Vincent van Gogh"
	}`)

	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
		return
	}

	checkExpanded := func(result []interface{}) {
		if len(result) == 1 {
			if v, match := result[0].(map[string]interface{}); match {
				if v, has := v["http://schema.org/name"]; has {
					if v, match := v.([]interface{}); match && len(v) == 1 {
						if v, match := v[0].(map[string]interface{}); match {
							if v, has := v["@value"]; has && v == "Vincent van Gogh" {
								return
							}
						}
					}
				}
			}
		}
		t.Error("IPFS document loaded did not expand document correctly")
		return
	}

	cidIpfs, err := sh.Add(bytes.NewReader(data))
	if err != nil {
		t.Error(err)
		return
	}

	cidIpld, err := sh.DagPut(data, "json", "cbor")
	if err != nil {
		t.Error(err)
		return
	}

	fmt.Println(cidIpfs, cidIpld)

	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = loader.NewShellDocumentLoader(sh)

	ipfsURI := "ipfs://" + cidIpfs
	ipfsResult, err := proc.Expand(ipfsURI, options)
	if err != nil {
		t.Error(err)
		return
	}
	checkExpanded(ipfsResult)

	dwebIpfsURI := "dweb:/ipfs/" + cidIpfs
	dwebIpfsResult, err := proc.Expand(dwebIpfsURI, options)
	if err != nil {
		t.Error(err)
		return
	}
	checkExpanded(dwebIpfsResult)

	dwebIpldURI := "dweb:/ipld/" + cidIpld
	dwebIpldResult, err := proc.Expand(dwebIpldURI, options)
	if err != nil {
		t.Error(err)
		return
	}
	checkExpanded(dwebIpldResult)
}

func TestSimpleIngest(t *testing.T) {
	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
		return
	}

	// Remove old db
	fmt.Println("removing path", path)
	if err := os.RemoveAll(path); err != nil {
		t.Error(err)
		return
	}

	db, err := styx.OpenDB(path)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	var data map[string]interface{}
	if err = json.Unmarshal(sampleData, &data); err != nil {
		t.Error(err)
		return
	}

	dl := loader.NewShellDocumentLoader(sh)

	store := styx.MakeShellDocumentStore(sh)

	if err = db.IngestJSONLd(data, dl, store); err != nil {
		t.Error(err)
		return
	}

	if err = db.Log(); err != nil {
		t.Error(err)
	}
}

func TestNTIngest(t *testing.T) {
	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
		return
	}

	// Remove old db
	fmt.Println("removing path", path)
	if err := os.RemoveAll(path); err != nil {
		t.Error(err)
		return
	}

	db, err := styx.OpenDB(path)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	dl := loader.NewShellDocumentLoader(sh)
	store := styx.MakeShellDocumentStore(sh)

	names, err := openFile("/samples/nt/names.json", dl, store, db)
	if err = db.IngestJSONLd(names, dl, store); err != nil {
		t.Error(err)
		return
	}

	// individuals, err := openFile("/samples/nt/individuals.json", dl, store, db)
	// if err = db.IngestJSONLd(individuals, dl, store); err != nil {
	// 	t.Error(err)
	// 	return
	// }

	if err = db.Log(); err != nil {
		t.Error(err)
	}
}

func TestQuery(t *testing.T) {
	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
		return
	} else if err := os.RemoveAll(path); err != nil {
		t.Error(err)
		return
	}

	db, err := styx.OpenDB(path)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	var data map[string]interface{}
	if err = json.Unmarshal(sampleData, &data); err != nil {
		t.Error(err)
		return
	}

	documentLoader := loader.NewShellDocumentLoader(sh)

	store := styx.MakeShellDocumentStore(sh)

	if err = db.IngestJSONLd(data, documentLoader, store); err != nil {
		t.Error(err)
		return
	}

	var queryData map[string]interface{}
	err = json.Unmarshal(sampleQuery, &queryData)
	if err != nil {
		t.Error(err)
		return
	}

	proc := ld.NewJsonLdProcessor()
	datasetOptions := styx.GetDatasetOptions(documentLoader)
	rdf, err := proc.ToRDF(queryData, datasetOptions)
	if err != nil {
		t.Error(err)
		return
	}

	quads := rdf.(*ld.RDFDataset).Graphs["@default"]

	fmt.Println("--- query graph ---")
	for _, quad := range quads {
		fmt.Printf(
			"  %s %s %s\n",
			quad.Subject.GetValue(),
			quad.Predicate.GetValue(),
			quad.Object.GetValue(),
		)
	}

	r := make(chan []*ld.Quad)
	go db.Query(quads, r)

	result := <-r

	fmt.Println("Result:")
	for _, quad := range result {
		fmt.Printf(
			"  %s %s %s\n",
			quad.Subject.GetValue(),
			quad.Predicate.GetValue(),
			quad.Object.GetValue(),
		)
	}
}

func openFile(path string, dl ld.DocumentLoader, store styx.DocumentStore, db *styx.DB) (doc map[string]interface{}, err error) {
	var dir string
	if dir, err = os.Getwd(); err != nil {
		return
	}

	var data []byte
	if data, err = ioutil.ReadFile(dir + path); err != nil {
		return
	}

	err = json.Unmarshal(data, &doc)
	return
}
