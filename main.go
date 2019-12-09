package main

import (
	"context"
	"log"
	"net/http"
	"os"

	ipfs "github.com/ipfs/go-ipfs-http-client"

	styx "github.com/underlay/styx/db"
)

var path = os.Getenv("STYX_PATH")
var host = os.Getenv("IPFS_HOST")

const defaultHost = "http://localhost:5001"

var port = os.Getenv("STYX_PORT")

var shError = "IPFS Daemon not running"

func main() {
	if host == "" {
		host = defaultHost
	}

	// Replace at your leisure
	api, err := ipfs.NewURLApiWithClient(host, http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	key, err := api.Key().Self(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	id := key.ID().String()

	db, err := styx.OpenDB(path, id, api)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	log.Fatal(db.Serve(port))
}
