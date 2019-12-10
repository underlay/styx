package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"

	files "github.com/ipfs/go-ipfs-files"
	ipfs "github.com/ipfs/go-ipfs-http-client"
	options "github.com/ipfs/interface-go-ipfs-core/options"
	ld "github.com/piprate/json-gold/ld"

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
	api, err := ipfs.NewURLApiWithClient("http://localhost:5001", http.DefaultClient)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	key, err := api.Key().Self(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Remove old db
	fmt.Println("removing path", styx.DefaultPath)
	if err := os.RemoveAll(styx.DefaultPath); err != nil {
		t.Fatal(err)
	}

	db, err := styx.OpenDB(styx.DefaultPath, key.ID().String(), api)
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

	if err = db.IngestJSONLd(ctx, data); err != nil {
		t.Error(err)
		return
	}

	if err = db.Log(); err != nil {
		t.Error(err)
	}
}

func testQuery(double bool, query []byte) (err error) {
	if err = os.RemoveAll(styx.DefaultPath); err != nil {
		return
	}

	api, err := ipfs.NewURLApiWithClient("http://localhost:5001", http.DefaultClient)
	if err != nil {
		return err
	}

	ctx := context.Background()

	key, err := api.Key().Self(ctx)
	if err != nil {
		return
	}

	id := key.ID().String()

	db, err := styx.OpenDB(styx.DefaultPath, id, api)
	if err != nil {
		return
	}

	defer db.Close()

	var data map[string]interface{}
	if err = json.Unmarshal(sampleData, &data); err != nil {
		return
	}

	if err = db.IngestJSONLd(ctx, data); err != nil {
		return
	}

	if double {
		var data2 map[string]interface{}
		if err = json.Unmarshal(sampleData2, &data2); err != nil {
			return
		}

		if err = db.IngestJSONLd(ctx, data2); err != nil {
			return
		}
	}

	db.Log()

	var queryData map[string]interface{}
	if err = json.Unmarshal(query, &queryData); err != nil {
		return
	}

	proc := ld.NewJsonLdProcessor()
	rdf, err := proc.ToRDF(queryData, db.Opts)
	if err != nil {
		return
	}

	size := uint32(len(rdf.(string)))
	reader := strings.NewReader(rdf.(string))

	resolved, err := db.FS.Add(
		ctx,
		files.NewReaderFile(reader),
		options.Unixfs.CidVersion(1),
		options.Unixfs.RawLeaves(true),
	)
	if err != nil {
		return
	}

	result, err := db.HandleMessage(ctx, resolved, size)
	if err != nil {
		return
	}

	jsonLdAPI := ld.NewJsonLdApi()

	normalized, err := jsonLdAPI.Normalize(result, db.Opts)
	fmt.Println(normalized, err)
	reader = strings.NewReader(normalized.(string))
	resolved, err = db.FS.Add(
		ctx,
		files.NewReaderFile(reader),
		options.Unixfs.CidVersion(1),
		options.Unixfs.RawLeaves(true),
		options.Unixfs.Pin(false),
	)
	fmt.Println(resolved)
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
				"@id": "dweb:/ipns/QmRybuaATHF1mnVy3VhhcbRhUedc3DkrpgMQBVEXx7oT9r",
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
				"@id": "dweb:/ipns/QmRybuaATHF1mnVy3VhhcbRhUedc3DkrpgMQBVEXx7oT9r",
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
