package main

import (
	"log"
	"os"

	ipfs "github.com/ipfs/go-ipfs-api"

	loader "github.com/underlay/go-ld-loader"
	styx "github.com/underlay/styx/db"
)

var path = os.Getenv("STYX_PATH")
var host = os.Getenv("IPFS_HOST")

const defaultHost = "localhost:5001"

var port = os.Getenv("STYX_PORT")

var shError = "IPFS Daemon not running"

func main() {
	if host == "" {
		host = defaultHost
	}

	// Replace at your leisure
	var sh = ipfs.NewShell(host)

	if !sh.IsUp() {
		log.Fatal(shError)
	}

	dl := loader.NewHTTPDocumentLoader(sh)
	store := styx.NewHTTPDocumentStore(sh)

	peerID, err := sh.ID()
	if err != nil {
		log.Fatal(err)
	}

	db, err := styx.OpenDB(path, peerID.ID, dl, store)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	log.Fatal(db.Serve(port))
}
