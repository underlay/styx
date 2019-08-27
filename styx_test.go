package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	cid "github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
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
		"name": ["John Doe", "Johnny Doe"],
		"birthDate": "1996-02-02",
		"knows": {
			"@id": "http://people.com/jane",
			"@type": "Person",
			"name": "Jane Doe",
			"familyName": { "@value": "Doe", "@language": "en" },
			"birthDate": "1995-01-01"
		}
	}
}`)

var simpleQuery = []byte(`{
	"@context": {
		"@vocab": "http://schema.org/"
	},
	"@type": "http://underlay.mit.edu/ns#Query",
	"@graph": {
		"@type": "Person",
		"birthDate": { },
		"knows": {
			"name": "Jane Doe"
		}
	}
}`)

var entityQuery = []byte(`{
	"@context": {
		"@vocab": "http://schema.org/",
		"prov": "http://www.w3.org/ns/prov#",
		"u": "http://underlay.mit.edu/ns#"
	},
	"@type": "http://underlay.mit.edu/ns#Query",
	"@graph": {
		"@type": "prov:Entity",
		"u:satisfies": {
			"@graph": {
				"@type": "Person",
				"birthDate": { },
				"knows": {
					"name": "Jane Doe"
				}
			}
		}
	}
}`)

var bundleQuery = []byte(`{
	"@context": {
		"@vocab": "http://schema.org/",
		"dcterms": "http://purl.org/dc/terms/",
		"prov": "http://www.w3.org/ns/prov#",
		"u": "http://underlay.mit.edu/ns#"
	},
	"@type": "http://underlay.mit.edu/ns#Query",
	"@graph": {
		"@type": "prov:Bundle",
		"dcterms:extent": 3,
		"u:enumerates": {
			"@graph": {
				"@type": "Person",
				"name": { }
			}
		}
	}
}`)

func TestIPFSDocumentLoader(t *testing.T) {
	data := []byte(`{
		"@context": { "@vocab": "http://schema.org/" },
		"name": "Vincent van Gogh"
	}`)

	// Replace at your leisure
	sh := ipfs.NewShell(defaultHost)

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

func TestIngest(t *testing.T) {
	// Replace at your leisure
	sh := ipfs.NewShell(defaultHost)

	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
		return
	}

	// Remove old db
	fmt.Println("removing path", defaultPath)
	if err := os.RemoveAll(defaultPath); err != nil {
		t.Error(err)
		return
	}

	db, err := styx.OpenDB(defaultPath)
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

func testQuery(query []byte) error {
	// Replace at your leisure
	sh := ipfs.NewShell(defaultHost)

	if !sh.IsUp() {
		return fmt.Errorf("IPFS Daemon not running")
	} else if err := os.RemoveAll(defaultPath); err != nil {
		return err
	}

	peerID, err := sh.ID()
	if err != nil {
		return err
	}

	db, err := styx.OpenDB(defaultPath)
	if err != nil {
		return err
	}

	defer db.Close()

	var data map[string]interface{}
	if err := json.Unmarshal(sampleData, &data); err != nil {
		return err
	}

	documentLoader := loader.NewShellDocumentLoader(sh)

	store := styx.MakeShellDocumentStore(sh)

	if err := db.IngestJSONLd(data, documentLoader, store); err != nil {
		return err
	}

	db.Log()

	var queryData map[string]interface{}
	if err := json.Unmarshal(query, &queryData); err != nil {
		return err
	}

	proc := ld.NewJsonLdProcessor()
	stringOptions := styx.GetStringOptions(documentLoader)
	rdf, err := proc.ToRDF(queryData, stringOptions)
	if err != nil {
		return err
	}

	hash, err := sh.Add(strings.NewReader(rdf.(string)))
	if err != nil {
		return err
	}

	c, err := cid.Decode(hash)
	if err != nil {
		return err
	}

	fmt.Println("CID", c)

	quads, graphs, err := styx.ParseMessage(strings.NewReader(rdf.(string)))

	fmt.Println("--- query graph ---")
	for _, quad := range quads {
		fmt.Printf(
			"  %s %s %s",
			quad.Subject.GetValue(),
			quad.Predicate.GetValue(),
			quad.Object.GetValue(),
		)
		if quad.Graph != nil {
			fmt.Print(" " + quad.Graph.GetValue())
		}
		fmt.Print("\n")
	}

	result := db.HandleMessage(peerID.ID, c, quads, graphs)

	fmt.Println("Result:")
	b, err := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(b))
	return err
}

func TestSimpleQuery(t *testing.T) {
	if err := testQuery(simpleQuery); err != nil {
		t.Error(err)
	}
}

func TestEntityQuery(t *testing.T) {
	if err := testQuery(entityQuery); err != nil {
		t.Error(err)
	}
}

func TestBundleQuery(t *testing.T) {
	if err := testQuery(bundleQuery); err != nil {
		t.Error(err)
	}
}

func TestNT(t *testing.T) {
	// Replace at your leisure
	sh := ipfs.NewShell(defaultHost)
	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
		return
	}

	peerID, err := sh.ID()
	if err != nil {
		t.Error(err)
		return
	}

	// Remove old db
	fmt.Println("removing path", defaultPath)
	if err := os.RemoveAll(defaultPath); err != nil {
		t.Error(err)
		return
	}

	db, err := styx.OpenDB(defaultPath)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	dl := loader.NewShellDocumentLoader(sh)
	store := styx.MakeShellDocumentStore(sh)

	// names, err := openFile("/samples/nt/names.json", dl, store)
	// if err != nil {
	// 	t.Error(err)
	// 	return
	// }

	// if err = db.IngestJSONLd(names, dl, store); err != nil {
	// 	t.Error(err)
	// 	return
	// }

	individuals, err := openFile("/samples/nt/individuals.json", dl, store)
	if err != nil {
		t.Error(err)
		return
	}

	if err = db.IngestJSONLd(individuals, dl, store); err != nil {
		t.Error(err)
		return
	}

	query, err := openFile("/samples/nt/small.json", dl, store)
	if err != nil {
		t.Error(err)
		return
	}

	documentLoader := loader.NewShellDocumentLoader(sh)

	proc := ld.NewJsonLdProcessor()
	stringOptions := styx.GetStringOptions(documentLoader)
	rdf, err := proc.ToRDF(query, stringOptions)

	if err != nil {
		t.Error(err)
	}

	quads, graphs, err := styx.ParseMessage(strings.NewReader(rdf.(string)))

	hash, err := sh.Add(strings.NewReader(rdf.(string)))
	if err != nil {
		t.Error(err)
		return
	}

	c, err := cid.Decode(hash)
	if err != nil {
		t.Error(err)
		return
	}

	fmt.Println("--- query graph ---")
	for _, quad := range quads {
		fmt.Printf(
			"  %s %s %s\n",
			quad.Subject.GetValue(),
			quad.Predicate.GetValue(),
			quad.Object.GetValue(),
		)
	}

	result := db.HandleMessage(peerID.ID, c, quads, graphs)

	fmt.Println("Result:")
	b, err := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(b))
}

func openFile(path string, dl ld.DocumentLoader, store styx.DocumentStore) (doc map[string]interface{}, err error) {
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
