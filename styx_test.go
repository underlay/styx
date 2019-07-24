package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	styx "github.com/underlay/styx/db"
	loader "github.com/underlay/styx/loader"
	types "github.com/underlay/styx/types"
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

func TestIngest(t *testing.T) {
	var data map[string]interface{}
	err := json.Unmarshal(sampleData, &data)
	if err != nil {
		t.Error(err)
		return
	}

	documentLoader := loader.NewShellDocumentLoader(sh)

	datasetOptions := styx.GetDatasetOptions(documentLoader)
	stringOptions := styx.GetStringOptions(documentLoader)

	proc := ld.NewJsonLdProcessor()
	api := ld.NewJsonLdApi()

	rdf, err := proc.Normalize(data, datasetOptions)
	if err != nil {
		t.Error(err)
		return
	}

	normalized, err := api.Normalize(rdf.(*ld.RDFDataset), stringOptions)
	if err != nil {
		t.Error(err)
		return
	}

	hash, err := sh.Add(bytes.NewReader([]byte(normalized.(string))))
	if err != nil {
		t.Error(err)
		return
	}

	log.Printf("Origin: %s\n", hash)

	cid, err := cid.Parse(hash)
	if err != nil {
		t.Error(err)
		return
	}

	// Remove old db
	fmt.Println("removing path", path)
	if err = os.RemoveAll(path); err != nil {
		t.Error(err)
		return
	}

	db, err := styx.OpenDB(path)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	for graph, quads := range rdf.(*ld.RDFDataset).Graphs {
		fmt.Println("ingesting", graph)
		if err := db.Ingest(cid, graph, quads); err != nil {
			t.Error(err)
			return
		}
	}

	err = db.Badger.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		var i int
		for iter.Seek(nil); iter.Valid(); iter.Next() {
			item := iter.Item()
			key := item.KeyCopy(nil)
			val, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}

			prefix := key[0]
			if bytes.Equal(key, types.CounterKey) {
				// Counter!
				log.Printf("Counter: %02d\n", binary.BigEndian.Uint64(val))
			} else if prefix == types.IndexPrefix {
				// Index key
				index := &types.Index{}
				if err = proto.Unmarshal(val, index); err != nil {
					return err
				}
				log.Printf("Index:\n  %s\n  %s\n", string(key[1:]), index.String())
			} else if prefix == types.ValuePrefix {
				// Value key
				value := &types.Value{}
				if err = proto.Unmarshal(val, value); err != nil {
					return err
				}
				id := binary.BigEndian.Uint64(key[1:])
				log.Printf("Value: %02d %s\n", id, value.GetValue())
			} else if _, has := types.TriplePrefixMap[prefix]; has {
				// Value key
				sourceList := &types.SourceList{}
				proto.Unmarshal(val, sourceList)
				log.Printf("Triple entry: %s %02d | %02d | %02d :: %s\n",
					string(key[0]),
					binary.BigEndian.Uint64(key[1:9]),
					binary.BigEndian.Uint64(key[9:17]),
					binary.BigEndian.Uint64(key[17:25]),
					types.PrintSources(sourceList.Sources),
				)
			} else if _, has := types.MinorPrefixMap[prefix]; has {
				// Minor key
				log.Printf("Minor entry: %s %02d | %02d :: %02d\n",
					string(key[0]),
					binary.BigEndian.Uint64(key[1:9]),
					binary.BigEndian.Uint64(key[9:17]),
					binary.BigEndian.Uint64(val),
				)
			} else if _, has := types.MajorPrefixMap[prefix]; has {
				// Major key
				log.Printf("Major entry: %s %02d | %02d :: %02d\n",
					string(key[0]),
					binary.BigEndian.Uint64(key[1:9]),
					binary.BigEndian.Uint64(key[9:17]),
					binary.BigEndian.Uint64(val),
				)
			}
			i++
		}
		log.Printf("Printed %02d database entries\n", i)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}

func TestQuery(t *testing.T) {
	var data map[string]interface{}
	err := json.Unmarshal(sampleData, &data)
	if err != nil {
		t.Error(err)
		return
	}

	documentLoader := loader.NewShellDocumentLoader(sh)

	datasetOptions := styx.GetDatasetOptions(documentLoader)
	stringOptions := styx.GetStringOptions(documentLoader)

	proc := ld.NewJsonLdProcessor()
	api := ld.NewJsonLdApi()

	rdf, err := proc.Normalize(data, datasetOptions)
	if err != nil {
		t.Error(err)
		return
	}

	normalized, err := api.Normalize(rdf.(*ld.RDFDataset), stringOptions)
	if err != nil {
		t.Error(err)
		return
	}

	hash, err := sh.Add(bytes.NewReader([]byte(normalized.(string))))
	if err != nil {
		t.Error(err)
		return
	}

	log.Printf("Origin: %s\n", hash)

	cid, err := cid.Parse(hash)
	if err != nil {
		t.Error(err)
		return
	}

	// Remove old db
	fmt.Println("removing path", path)
	if err = os.RemoveAll(path); err != nil {
		t.Error(err)
		return
	}

	db, err := styx.OpenDB(path)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	for graph, quads := range rdf.(*ld.RDFDataset).Graphs {
		fmt.Println("ingesting", graph)
		if err := db.Ingest(cid, graph, quads); err != nil {
			t.Error(err)
			return
		}
	}

	var queryData map[string]interface{}
	err = json.Unmarshal(sampleQuery, &queryData)
	if err != nil {
		t.Error(err)
		return
	}

	rdf, err = proc.ToRDF(queryData, datasetOptions)
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
