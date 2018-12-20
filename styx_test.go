package styx

import (
	"bytes"
	"fmt"
	"log"
	"testing"

	"encoding/json"

	"github.com/dgraph-io/badger"
	ipfs "github.com/ipfs/go-ipfs-api"
	"github.com/piprate/json-gold/ld"
)

func TestIPFS(t *testing.T) {
	shell := ipfs.NewShell("localhost:5001")
	data := map[string]interface{}{
		"@context": map[string]interface{}{
			"@vocab": "http://schema.org/",
		},
		"name": "Vincent van Gogh",
	}

	b, err := json.Marshal(data)
	if err != nil {
		log.Fatalln(err)
	}

	cid, err := shell.Add(bytes.NewReader(b))
	if err != nil {
		log.Fatalln(err)
	}

	uri := "ipfs://" + cid

	fmt.Println("got uri", uri)
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(shell)

	res, err := proc.Expand(uri, options)
	fmt.Println(res, err)
}

func TestIPLD(t *testing.T) {
	shell := ipfs.NewShell("localhost:5001")
	data := map[string]interface{}{
		"@context": map[string]interface{}{
			"@vocab": "http://schema.org/",
		},
		"@type": "Person",
		"name":  "Vincent van Gogh",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		log.Fatalln(err)
	}

	cid, err := shell.DagPut(bytes, "json", "cbor")
	if err != nil {
		log.Fatalln(err)
	}

	uri := "dweb:/ipld/" + cid

	fmt.Println("got uri", uri)

	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(shell)

	res, err := proc.Expand(uri, options)
	fmt.Println(res, err)
}

func TestIngest(t *testing.T) {
	u := "ipfs://QmZUjboQkj5xyrrv1ty8zb8QvXDzAh6yE3D9KUXZpKh3S9"

	// Create shell
	sh := ipfs.NewShell("localhost:5001")

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ingest(u, db, sh)
	// var i int
	// for i < 600 {
	// 	fmt.Println("doing the thing")
	// 	i++
	// 	ingest(data, db, sh)
	// }
}

func TestInsert(t *testing.T) {
	// data
	cid := "QmbmnUHecmF5MEBETY5J2n2BEXxYssAa4pceKKg59ai9tZ"
	data := map[string]interface{}{
		"@context": map[string]interface{}{
			"@vocab": "http://schema.org/",
		},
		"@graph": map[string]interface{}{
			"name": "Joel Gustafson",
			"age":  "22",
			"@graph": map[string]interface{}{
				"friend": "other person",
			},
		},
	}
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	rdf, err := proc.Normalize(data, options)
	if err != nil {
		log.Fatalln(err)
	}

	dataset := rdf.(*ld.RDFDataset)

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Update(func(txn *badger.Txn) error {
		var i int
		var err error
		for i < 600 {
			i++
			err = insert(cid, dataset, txn)
		}
		return err
	})
}

func TestKey(t *testing.T) {
	// Create key
	k := "a\tQmbmnUHecmF5MEBETY5J2n2BEXxYssAa4pceKKg59ai9tZ:c14n0\t<http://schema.org/age>"
	key := []byte(k)

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
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
	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
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
	data := map[string]interface{}{
		"@context": map[string]interface{}{
			"@vocab": "http://schema.org/",
		},
		"@type": "Person",
		"name":  "Joel",
		"age":   "22",
		"friend": map[string]interface{}{
			"@type": "Person",
			"name":  "Colin",
			"age":   "24",
		},
	}

	sh := ipfs.NewShell("localhost:5001")

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	fmt.Println("about to do the thing")
	ingest(data, db, sh)

	fmt.Println("we did the freaking thing")

	// Okay now the data has been ingested
	// We want to construct a query
	q := map[string]interface{}{
		"@context": map[string]interface{}{
			"@vocab": "http://schema.org/",
		},
		"friend": map[string]interface{}{
			"@type": "Person",
			"name":  "Colin",
		},
		"age": map[string]interface{}{},
	}

	err = query(q, sh, db, func(as AssignmentStack) error {
		fmt.Println("wow")
		printAssignmentStack(as)
		return nil
	})
	if err != nil {
		fmt.Println("there was an error:", err.Error())
		// log.Fatalln(err)
	}
}
