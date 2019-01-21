package styx

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"encoding/json"

	badger "github.com/dgraph-io/badger"
	proto "github.com/golang/protobuf/proto"
	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"
)

// Replace at your leisure
const path = "/tmp/badger"

// Replace at your leisure
var sh = ipfs.NewShell("localhost:5001")
var sampleData = []byte(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "DigitalDocument",
	"age": 33,
	"@graph": {
		"name": "Joel",
		"age": [22, 23],
		"friend": {
			"@id": "http://example.org/gabriel",
			"age": [22],
			"name": {
				"@value": "Gabriel",
				"@language": "es"
			}
		}
	}
}`)

func openDB(t *testing.T, clean bool) *badger.DB {
	// Sanity check for the daemon
	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
	}

	// Remove old db
	if clean {
		if err := os.RemoveAll(path); err != nil {
			t.Error(err)
		}
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
	json.Unmarshal(sampleData, &data)

	db := openDB(t, true)
	defer db.Close()

	origin, err := ingest(data, db, sh)
	if err != nil {
		t.Error(err)
	}

	fmt.Printf("Origin: %s\n", origin)

	err = db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()
		var key, val []byte
		var i int
		for iter.Seek(nil); iter.Valid(); iter.Next() {
			item := iter.Item()
			key = item.KeyCopy(key)
			val, err := item.ValueCopy(val)
			if err != nil {
				return err
			}
			prefix := key[0]
			if _, has := valuePrefixMap[prefix]; has {
				// Value key
				sourceList := &SourceList{}
				proto.Unmarshal(val, sourceList)
				// bytes, _ := json.MarshalIndent(sourceList.Sources, "  ", "  ")
				fmt.Printf("Value entry\n  %s\n  %s\n", string(key), sourceList.toCompactString())
			} else if _, has := minorPrefixMap[prefix]; has {
				// Minor key
				fmt.Printf("Minor entry\n  %s\n  %v\n", string(key), val)
			} else if _, has := majorPrefixMap[prefix]; has {
				// Major key
				fmt.Printf("Major entry\n  %s\n  %v\n", string(key), val)
			} else if _, has := indexPrefixMap[prefix]; has {
				// Index key
				fmt.Printf("Index entry\n  %s\n  %v\n", string(key), val)
			}
			i++
		}
		fmt.Printf("Printed %d database entries\n", i)
		return nil
	})
	if err != nil {
		t.Error(err)
	}
}
