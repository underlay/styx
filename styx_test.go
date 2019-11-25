package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	cid "github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
	multibase "github.com/multiformats/go-multibase"
	ld "github.com/piprate/json-gold/ld"

	loader "github.com/underlay/go-ld-loader"
	styx "github.com/underlay/styx/db"
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

var sampleData2 = []byte(`{
	"@context": {
		"@vocab": "http://schema.org/",
		"xsd": "http://www.w3.org/2001/XMLSchema#",
		"birthDate": { "@type": "xsd:date" }
	},
	"@type": "Person",
	"name": "Johnanthan Appleseed",
	"birthDate": "1780-01-10",
	"knows": { "@id": "http://people.com/jane" }
}`)

func TestIngest(t *testing.T) {
	// Replace at your leisure
	sh := ipfs.NewShell(loader.DefaultHTTPAddress)

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
	fmt.Println("removing path", styx.DefaultPath)
	if err := os.RemoveAll(styx.DefaultPath); err != nil {
		t.Error(err)
		return
	}

	dl := loader.NewHTTPDocumentLoader(sh)

	store := styx.NewHTTPDocumentStore(sh)

	db, err := styx.OpenDB(styx.DefaultPath, peerID.ID, dl, store)
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

	if err = db.IngestJSONLd(data); err != nil {
		t.Error(err)
		return
	}

	if err = db.Log(); err != nil {
		t.Error(err)
	}
}

func testQuery(double bool, query []byte) (err error) {
	// Replace at your leisure
	sh := ipfs.NewShell(loader.DefaultHTTPAddress)

	if !sh.IsUp() {
		return fmt.Errorf("IPFS Daemon not running")
	} else if err = os.RemoveAll(styx.DefaultPath); err != nil {
		return
	}

	peerID, err := sh.ID()
	if err != nil {
		return
	}

	dl := loader.NewHTTPDocumentLoader(sh)

	store := styx.NewHTTPDocumentStore(sh)

	db, err := styx.OpenDB(styx.DefaultPath, peerID.ID, dl, store)
	if err != nil {
		return
	}

	defer db.Close()

	var data map[string]interface{}
	if err = json.Unmarshal(sampleData, &data); err != nil {
		return
	}

	if err = db.IngestJSONLd(data); err != nil {
		return
	}

	if double {
		var data2 map[string]interface{}
		if err = json.Unmarshal(sampleData2, &data2); err != nil {
			return
		}

		if err = db.IngestJSONLd(data2); err != nil {
			return
		}
	}

	db.Log()

	var queryData map[string]interface{}
	if err = json.Unmarshal(query, &queryData); err != nil {
		return
	}

	proc := ld.NewJsonLdProcessor()
	stringOptions := styx.GetStringOptions(dl)
	rdf, err := proc.ToRDF(queryData, stringOptions)
	if err != nil {
		return
	}

	size := uint32(len(rdf.(string)))

	c, err := store.Put(strings.NewReader(rdf.(string)))
	if err != nil {
		return
	}

	result, err := db.HandleMessage(c, size)
	if err != nil {
		return
	}

	api := ld.NewJsonLdApi()

	// doc, err := api.FromRDF(result, stringOptions)
	// s, err := json.MarshalIndent(doc, "", "  ")
	// fmt.Println(string(s), err)

	normalized, err := api.Normalize(result, stringOptions)
	fmt.Println(normalized, err)
	s1, err := sh.Add(strings.NewReader(normalized.(string)), ipfs.RawLeaves(true), ipfs.Pin(false))
	if err != nil {
		return
	}
	c2, err := cid.Decode(s1)
	if err != nil {
		return
	}
	s2, err := c2.StringOfBase(multibase.Base32)
	if err != nil {
		return
	}
	fmt.Println(s2)
	return
}

func TestSPO(t *testing.T) {
	if err := testQuery(false, []byte(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "http://underlay.mit.edu/ns#Query",
	"@graph": {
		"@id": "http://people.com/jane",
		"name": { }
	}
}`)); err != nil {
		t.Error(err)
	}
}

func TestOPS(t *testing.T) {
	if err := testQuery(false, []byte(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "http://underlay.mit.edu/ns#Query",
	"@graph": {
		"name": "Jane Doe"
	}
}`)); err != nil {
		t.Error(err)
	}
}

func TestSimpleQuery(t *testing.T) {
	if err := testQuery(false, []byte(`{
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
}`)); err != nil {
		t.Error(err)
	}
}

func TestEntityQuery(t *testing.T) {
	if err := testQuery(false, []byte(`{
	"@context": {
		"@vocab": "http://schema.org/",
		"dcterms": "http://purl.org/dc/terms/",
		"prov": "http://www.w3.org/ns/prov#",
		"u": "http://underlay.mit.edu/ns#",
		"u:index": { "@container": "@list" },
		"u:domain": { "@container": "@list" }
	},
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Entity",
		"dcterms:extent": 3,
		"u:domain": [],
		"u:index": [],
		"u:satisfies": {
			"@graph":  {
				"@type": "Person",
				"knows": {
					"name": "Jane Doe"
				}
			}
		}
	}
}`)); err != nil {
		t.Error(err)
	}
}

func TestGraphQuery(t *testing.T) {
	if err := testQuery(true, []byte(`{
	"@context": {
		"dcterms": "http://purl.org/dc/terms/",
		"prov": "http://www.w3.org/ns/prov#",
		"ldp": "http://www.w3.org/ns/ldp#",
		"u": "http://underlay.mit.edu/ns#",
		"u:index": { "@container": "@list" },
		"u:domain": { "@container": "@list" }
	},
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Entity",
		"dcterms:extent": 3,
		"u:domain": [],
		"u:index": [],
		"u:satisfies": {
			"@graph": {
				"@id": "dweb:/ipns/QmYxMiLd4GXeW8FTSFGUiaY8imCksY6HH9LBq86gaFiwXG",
				"ldp:member": { }
			}
		}
	}
}`)); err != nil {
		t.Error(err)
	}
}

func TestGraphIndexQuery(t *testing.T) {
	if err := testQuery(true, []byte(`{
	"@context": {
		"dcterms": "http://purl.org/dc/terms/",
		"prov": "http://www.w3.org/ns/prov#",
		"ldp": "http://www.w3.org/ns/ldp#",
		"u": "http://underlay.mit.edu/ns#",
		"u:index": { "@container": "@list" },
		"u:domain": { "@container": "@list" }
	},
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Entity",
		"dcterms:extent": 3,
		"u:domain": [{ "@id": "_:member" }],
		"u:index": [{ "@id": "ul:/ipfs/bafybeibwbdi2renonku7zsl7yv3ww6cumoqs7thipex2slfp7j5qtfwbhm" }],
		"u:satisfies": {
			"@graph": {
				"@id": "dweb:/ipns/QmYxMiLd4GXeW8FTSFGUiaY8imCksY6HH9LBq86gaFiwXG",
				"ldp:member": { "@id": "_:member" }
			}
		}
	}
}`)); err != nil {
		t.Error(err)
	}
}

func TestDomainQuery(t *testing.T) {
	if err := testQuery(true, []byte(`{
	"@context": {
		"@vocab": "http://schema.org/",
		"dcterms": "http://purl.org/dc/terms/",
		"prov": "http://www.w3.org/ns/prov#",
		"u": "http://underlay.mit.edu/ns#",
		"u:index": { "@container": "@list" },
		"u:domain": { "@container": "@list" }
	},
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Entity",
		"dcterms:extent": 4,
		"u:domain": [{ "@id": "_:b0" }],
		"u:index": [],
		"u:satisfies": {
			"@graph": {
				"@id": "_:b0",
				"name": { "@id": "_:b1" }
			}
		}
	}
}`)); err != nil {
		t.Error(err)
	}
}

func TestIndexQuery(t *testing.T) {
	if err := testQuery(true, []byte(`{
	"@context": {
		"@vocab": "http://schema.org/",
		"dcterms": "http://purl.org/dc/terms/",
		"prov": "http://www.w3.org/ns/prov#",
		"u": "http://underlay.mit.edu/ns#",
		"u:index": { "@container": "@list" },
		"u:domain": { "@container": "@list" }
	},
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Entity",
		"dcterms:extent": 4,
		"u:domain": [{ "@id": "_:b0" }],
		"u:index": [{ "@id": "ul:/ipfs/bafybeibwbdi2renonku7zsl7yv3ww6cumoqs7thipex2slfp7j5qtfwbhm#_:c14n1" }],
		"u:satisfies": {
			"@graph": {
				"@id": "_:b0",
				"name": { "@id": "_:b1" }
			}
		}
	}
}`)); err != nil {
		t.Error(err)
	}
}

func openFile(path string, dl ld.DocumentLoader) (doc map[string]interface{}, err error) {
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
