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

func main() {
	db := openDB(nil, true)
	http.Handle("/", http.FileServer(http.Dir("./static")))
	http.HandleFunc("/ingest", func(w http.ResponseWriter, r *http.Request) { handleIngest(db, w, r) })
	http.HandleFunc("/query", func(w http.ResponseWriter, r *http.Request) { handleQuery(db, w, r) })
	fmt.Println("Listening on port 8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

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

func handleIngest(db *badger.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		decoder := json.NewDecoder(r.Body)
		var doc interface{}
		err := decoder.Decode(&doc)
		if err == nil {
			hash, err := Ingest(doc, db, sh)
			if err != nil {
				buf, _ := json.Marshal(map[string]string{"error": err.Error()})
				fmt.Fprintf(w, "%s\n", string(buf))
			} else {
				buf, _ := json.Marshal(map[string]string{"cid": hash})
				fmt.Fprintf(w, "%s\n", string(buf))
			}
		}
	} else {
		http.Error(w, "Method Not Allowed", 405)
	}
}

func handleQuery(db *badger.DB, w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		decoder := json.NewDecoder(r.Body)
		var query interface{}
		err := decoder.Decode(&query)
		if err == nil {
			err := Query(query, func(result interface{}) error {
				bytes, _ := json.Marshal(result)
				fmt.Fprintf(w, "%s\n", string(bytes))
				return nil
			}, db, sh)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
	} else {
		http.Error(w, r.Method, http.StatusMethodNotAllowed)
	}
}
