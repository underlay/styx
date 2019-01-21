package styx

import (
	"bytes"
	"fmt"
	"testing"

	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"
)

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
