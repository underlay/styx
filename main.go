package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"testing"

	badger "github.com/dgraph-io/badger"
	ipfs "github.com/ipfs/go-ipfs-api"
)

// Replace at your leisure
const path = "/tmp/badger"

// Replace at your leisure
var sh = ipfs.NewShell("localhost:5001")

func openDB(t *testing.T, clean bool) *badger.DB {
	// Sanity check for the daemon
	if !sh.IsUp() {
		t.Error("IPFS Daemon not running")
		return nil
	}

	// Remove old db
	if clean {
		if err := os.RemoveAll(path); err != nil {
			t.Error(err)
			return nil
		}
	}

	// Create DB
	opts := badger.DefaultOptions
	opts.Dir = path
	opts.ValueDir = path

	db, err := badger.Open(opts)
	if err != nil {
		t.Error(err)
		return nil
	}

	return db
}

func handleQuery(db *badger.DB, w http.ResponseWriter, r *http.Request) {
	fmt.Println("handling query")
	if r.Method == http.MethodPost {
		decoder := json.NewDecoder(r.Body)
		var query interface{}
		err := decoder.Decode(&query)
		fmt.Println("query", err)
		fmt.Println(query)
		if err == nil {
			Query(query, func(result interface{}) error {
				buf, _ := json.Marshal(result)
				fmt.Println("got result", string(buf))
				fmt.Fprintf(w, "%s\n", string(buf))
				return nil
			}, db, sh)
		}
	}
}

func handleIngest(db *badger.DB, w http.ResponseWriter, r *http.Request) {
	fmt.Println("handling ingest")
	if r.Method == http.MethodPost {
		decoder := json.NewDecoder(r.Body)
		var doc interface{}
		err := decoder.Decode(&doc)
		fmt.Println("doc", err)
		fmt.Println(doc)
		if err == nil {
			result, err := Ingest(doc, db, sh)
			fmt.Println("got result", result)
			if err != nil {
				buf, _ := json.Marshal(map[string]string{"error": err.Error()})
				fmt.Fprintf(w, "%s\n", string(buf))
			} else {
				buf, _ := json.Marshal(map[string]string{"result": result})
				fmt.Fprintf(w, "%s\n", string(buf))
			}
		}
	}
}

func main() {
	db := openDB(nil, true)
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) { handleIngest(db, w, r) })
	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) { handleQuery(db, w, r) })
	fmt.Println("Listening on port 8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
