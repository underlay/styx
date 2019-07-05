package main

import (
	"fmt"
	"log"
	"net/http"

	ipfs "github.com/ipfs/go-ipfs-api"
)

// Replace at your leisure
const path = "/tmp/badger"

// Replace at your leisure
var sh = ipfs.NewShell("localhost:5001")
var shError = "IPFS Daemon not running"

func main() {

	http.Handle("/", http.FileServer(http.Dir(".")))
	fmt.Println("Listening on port 8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
