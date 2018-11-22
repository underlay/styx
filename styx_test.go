package styx

import (
	fmt "fmt"
	"log"
	"testing"

	"github.com/dgraph-io/badger"
	ipfs "github.com/ipfs/go-ipfs-api"
	"github.com/piprate/json-gold/ld"
)

func TestLoader(t *testing.T) {
	u := "dweb:/ipld/zdpuAmz1n5w7REfwt5f7uCheRr6Rz5ujXwqMte6vo4Z44QMCv"

	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.DocumentLoader = NewIPFSDocumentLoader(nil)
	res, err := proc.Expand(u, options)
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
}

func TestKey(t *testing.T) {
	// Create key
	k := "a\tQmVDZbsPNWcNTDBbrmvbog1KiHCSnVpz6Cxb2je1CqiDnD:c14n0\t<http://schema.org/age>"
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
			return err
		}
		fmt.Println(item.String())
		return item.Value(func(val []byte) error {
			fmt.Println(string(val))
			return nil
		})
	})
}
