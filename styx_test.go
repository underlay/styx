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
	var data interface{}
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
	defer db.Close()

	err = ingest(data, db, sh)
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
	var data interface{}
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

	// Remove old db
	if err = os.RemoveAll(path); err != nil {
		t.Error(err)
	}

	// // Create DB
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		t.Error(err)
	}
	defer db.Close()

	// db.Update(func(txn *badger.Txn) error {
	// 	var i int
	// 	var err error
	// 	for i < 600 {
	// 		i++
	// 		err = insert(cid, dataset, txn)
	// 	}
	// 	return err
	// })
}

func TestKey(t *testing.T) {
	// Create key
	k := "a\tQmbmnUHecmF5MEBETY5J2n2BEXxYssAa4pceKKg59ai9tZ:c14n0\t<http://schema.org/age>"
	key := []byte(k)

	// Do not remove old db

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	defer db.Close()

	// Check the value
	db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			fmt.Println("error", err)
			return err
		}
		b, err := item.ValueCopy(nil)
		fmt.Println("got item!", b)
		fmt.Println(item.String())
		return item.Value(func(val []byte) error {
			fmt.Println(val)
			return nil
		})
	})
}

func TestKeyIteration(t *testing.T) {
	// Create key
	k := "a\tQmbmnUHecmF5MEBETY5J2n2BEXxYssAa4pceKKg59ai9tZ:c14n0\t<http://schema.org/age>"
	key := []byte(k)

	// Do not remove old db

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	// Check the value
	l := len(key)
	db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(key); it.ValidForPrefix(key); it.Next() {
			item := it.Item()
			k := item.Key()
			fmt.Println(k[l:])
		}
		return nil
	})
}

func printAssignmentStack(as AssignmentStack) {
	fmt.Println("--- stack ---")
	deps, _ := json.Marshal(as.deps)
	fmt.Println(string(deps))
	for i, m := range as.maps {
		fmt.Printf("map %d:\n", i)
		for k, v := range m {
			b, _ := json.Marshal(v)
			fmt.Printf("  %s: "+string(b)+"\n", k)
			fmt.Println("        " + string(v.Value))
		}
	}
}

func printCodex(codex Codex) {
	fmt.Println("--- codex ---")
	s, _ := json.Marshal(codex.Single)
	d, _ := json.Marshal(codex.Double)
	t, _ := json.Marshal(codex.Triple)
	fmt.Println(string(s))
	fmt.Println(string(d))
	fmt.Println(string(t))
}

func TestPromote(t *testing.T) {
	data := map[string]interface{}{
		"@context": map[string]interface{}{
			"@vocab": "http://schema.org/",
		},
		"@type": "Person",
		"name":  "Vincent van Gogh",
		"friend": map[string]interface{}{
			"name": map[string]interface{}{},
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
	if err := os.RemoveAll(path); err != nil {
		log.Fatalln(err)
	}

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	var data map[string]interface{}
	err = json.Unmarshal([]byte(`{
		"@context": {	"@vocab": "http://schema.org/" },
		"@id": "_:joel",
		"@type": "Person",
		"name": "Joel",
		"age": 22,
		"friend": {
			"@type": "Person",
			"name": "Colin",
			"age": 24
		}
	}`), &data)

	if err != nil {
		t.Error(err)
	}

	ingest(data, db, sh)

	// Okay now the data has been ingested
	// We want to construct a query
	var q map[string]interface{}
	err = json.Unmarshal([]byte(`{
		"@context": { "@vocab": "http://schema.org/" },
		"@type": "Person",
		"name": "Joel",
		"age": {}
	}`), &q)

	if err != nil {
		t.Error(err)
	}

	err = query(q, sh, db, func(as AssignmentStack) error {
		// printAssignmentStack(as)
		return nil
	})

	if err != nil {
		t.Error(err)
	}
}
