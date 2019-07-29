package main

import (
	"log"
	"net/http"
	"os"

	ipfs "github.com/ipfs/go-ipfs-api"
)

// Replace at your leisure
const tempPath = "/tmp/styx"

var path = os.Getenv("STYX_PATH")

// Replace at your leisure
var sh = ipfs.NewShell("localhost:5001")
var shError = "IPFS Daemon not running"

func main() {
	if path == "" {
		path = tempPath
	}

	if !sh.IsUp() {
		log.Fatal(shError)
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	http.Handle("/", http.FileServer(http.Dir(wd+"/www")))
	log.Println("Listening on port 8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
