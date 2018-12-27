package styx

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"

	"encoding/json"

	"github.com/dgraph-io/badger"
	ipfs "github.com/ipfs/go-ipfs-api"
	"github.com/piprate/json-gold/ld"
)

// Replace at your leisure
const path = "/tmp/badger"

// Replace at your leisure
var sh = ipfs.NewShell("localhost:5001")

func openDB(t *testing.T) *badger.DB {
	// Sanity check for the daemon
	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
	}

	// Remove old db
	if err := os.RemoveAll(path); err != nil {
		t.Error(err)
	}

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path

	db, err := badger.Open(opts)
	if err != nil {
		t.Error(err)
	}

	return db
}

func TestIPFSDocumentLoader(t *testing.T) {
	data := []byte(`{
		"@context": { "@vocab": "http://schema.org/" },
		"name": "Vincent van Gogh"
	}`)

	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
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
		fmt.Println("GONNA ERROR SIGNAL")
		t.Error("IPFS document loaded did not expand document correctly")
	}

	cidIpfs, err := sh.Add(bytes.NewReader(data))
	if err != nil {
		t.Error(err)
	}

	cidIpld, err := sh.DagPut(data, "json", "cbor")
	if err != nil {
		t.Error(err)
	}

	fmt.Println(cidIpfs, cidIpld)

	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(sh)

	ipfsURI := "ipfs://" + cidIpfs
	ipfsResult, err := proc.Expand(ipfsURI, options)
	if err != nil {
		t.Error(err)
	}
	checkExpanded(ipfsResult)

	fmt.Println("check passed")

	dwebIpfsURI := "dweb:/ipfs/" + cidIpfs
	dwebIpfsResult, err := proc.Expand(dwebIpfsURI, options)
	if err != nil {
		t.Error(err)
	}
	checkExpanded(dwebIpfsResult)

	ipldURI := "ipfs://" + cidIpld
	ipldResult, err := proc.Expand(ipldURI, options)
	if err != nil {
		t.Error(err)
	}
	checkExpanded(ipldResult)

	dwebIpldURI := "dweb:/ipfs/" + cidIpld
	dwebIpldResult, err := proc.Expand(dwebIpldURI, options)
	if err != nil {
		t.Error(err)
	}
	checkExpanded(dwebIpldResult)
}

func TestIngest(t *testing.T) {
	var data map[string]interface{}
	json.Unmarshal([]byte(`{
		"@context": { "@vocab": "http://schema.org/" },
		"@type": "DigitalDocument",
		"@graph": {
			"name": "Joel",
			"age": 22,
			"friend": {
				"@id": "http://example.org/gabriel",
				"name": {
					"@value": "Gabriel",
					"@language": "es"
				}
			}
		}
	}`), &data)

	db := openDB(t)
	defer db.Close()

	err := ingest(data, db, sh)
	if err != nil {
		t.Error(err)
	}

	err = db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		var key, val []byte
		for iter.Seek(nil); iter.Valid(); iter.Next() {
			item := iter.Item()
			key = item.KeyCopy(key)
			val, err = item.ValueCopy(val)
			if err != nil {
				return err
			}
			permutation := key[0]
			if _, has := valuePrefixMap[permutation]; has {
				// Value key
				fmt.Println("Value entry")
				fmt.Print("  ")
				fmt.Print(string(key[:len(key)-8]))
				fmt.Print("\t")
				fmt.Print(key[len(key)-8:])
				fmt.Print("\n")
				fmt.Print("  ")
				fmt.Print(string(val))
				fmt.Print("\n")
			} else if _, has := minorPrefixMap[permutation]; has {
				// Minor key
				fmt.Println("Minor entry")
				fmt.Print("  ")
				fmt.Print(string(key))
				fmt.Print("\n")
				fmt.Print("  ")
				fmt.Print(val)
				fmt.Print("\n")
			} else if _, has := majorPrefixMap[permutation]; has {
				// Major key
				fmt.Println("Minor entry")
				fmt.Print("  ")
				fmt.Print(string(key))
				fmt.Print("\n")
				fmt.Print("  ")
				fmt.Print(val)
				fmt.Print("\n")
			}
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestInsert(t *testing.T) {
	var data map[string]interface{}
	json.Unmarshal([]byte(`{
		"@context": { "@vocab": "http://schema.org/" },
		"@type": "DigitalDocument",
		"@graph": {
			"name": "Joel",
			"age": 22,
			"friend": {
				"@id": "http://example.org/gabriel",
				"name": {
					"@value": "Gabriel",
					"@language": "es"
				}
			}
		}
	}`), &data)

	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	rdf, err := proc.Normalize(data, options)
	if err != nil {
		t.Error(err)
	}

	dataset := rdf.(*ld.RDFDataset)
	fmt.Println(dataset)

	db := openDB(t)
	defer db.Close()
}

func TestPromote(t *testing.T) {
	var data map[string]interface{}
	json.Unmarshal([]byte(`{
		"@context": { "@vocab": "http://schema.org/" },
		"@type": "Person",
		"name": "Vincent van Gogh",
		"friend": {
			"name": {}
		}
	}`), &data)

	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	rdf, err := proc.ToRDF(data, options)
	if err != nil {
		t.Error(err)
	}

	dataset := rdf.(*ld.RDFDataset)
	printDataset(dataset)

	as := getAssignmentStack(dataset)
	printAssignmentStack(as)
}

func TestConstrain(t *testing.T) {
	data := map[string]interface{}{
		"@context": map[string]interface{}{
			"@vocab": "http://schema.org/",
		},
		"name": "Joel",
		"friend": map[string]interface{}{
			"name": "Joel 2",
		},
	}
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	rdf, err := proc.ToRDF(data, options)
	if err != nil {
		log.Fatalln(err)
	}

	dataset := rdf.(*ld.RDFDataset)

	for _, quad := range dataset.Graphs["@default"] {
		fmt.Println(quad.Subject.GetValue(), quad.Predicate.GetValue(), quad.Object.GetValue())
	}

	codex := getCodex(dataset)
	as := AssignmentStack{maps: []AssignmentMap{}, deps: map[string]int{}}
	printCodex(codex)
	printAssignmentStack(as)
	as, codex = haveDinner(as, codex)
	printCodex(codex)
	printAssignmentStack(as)
}

func TestSolve(t *testing.T) {
	var data map[string]interface{}
	err := json.Unmarshal([]byte(`{
		"@context": {	"@vocab": "http://schema.org/" },
		"@graph": [
			{
				"@type": "Tulpa",
				"name": "Someone else"
			},
			{
				"@type": "Person",
				"name": "A person"
			},
			{
				"@type": "Tulpa",
				"name": "Colin",
				"age": 4,
				"friend": {
					"@type": "Tulpa",
					"name": "Joel",
					"age": 2
				}
			},
			{
				"@type": "Person",
				"name": "Joel",
				"age": 22,
				"friend": {
					"@type": "Person",
					"name": "Colin",
					"age": 24
				}
			}
		]
	}`), &data)

	if err != nil {
		t.Error(err)
	}

	db := openDB(t)
	defer db.Close()

	ingest(data, db, sh)

	// Okay now the data has been ingested
	// We want to construct a query
	var q Graph
	err = json.Unmarshal([]byte(`{
		"@context": { "@vocab": "http://schema.org/" },
		"age": {},
		"name": "Colin",
		"friend": {
			"age": {},
			"name": "Joel"
		}
	}`), &q)

	if err != nil {
		t.Error(err)
	}

	err = query(q, sh, db, func(result, prov, fail map[string]interface{}) error {
		if result != nil {
			b, err := json.MarshalIndent(result, "", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(b))
		}
		return nil
	})

	if err != nil {
		t.Error(err)
	}
}
