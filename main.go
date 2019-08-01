package main

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"

	styx "github.com/underlay/styx/db"
	loader "github.com/underlay/styx/loader"
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

	peerID, err := sh.ID()
	if err != nil {
		log.Fatal(err)
	}

	db, err := styx.OpenDB(path)
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	proc := ld.NewJsonLdProcessor()
	dl := loader.NewShellDocumentLoader(sh)
	options := styx.GetStringOptions(dl)

	store := styx.MakeShellDocumentStore(sh)

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	dir := http.Dir(wd + "/www")
	fs := http.FileServer(dir)
	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" && req.URL.Path == "/" {
			t := req.Header.Get("Content-Type")
			var cid cid.Cid
			var reader io.Reader
			if t == "application/ld+json" {
				decoder := json.NewDecoder(req.Body)
				var doc interface{}
				if err := decoder.Decode(&doc); err != nil {
					res.WriteHeader(400)
					return
				}

				// Convert to RDF
				rdf, err := proc.Normalize(doc, options)
				if s, is := rdf.(string); !is || err != nil {
					res.WriteHeader(400)
					return
				} else if c, err := store(strings.NewReader(s)); err != nil {
					res.WriteHeader(500)
					return
				} else {
					cid = c
					reader = strings.NewReader(s)
				}
			} else if t == "application/n-quads" {
				if b, err := ioutil.ReadAll(req.Body); err != nil {
					res.WriteHeader(500)
					return
				} else if c, err := store(bytes.NewReader(b)); err != nil {
					res.WriteHeader(400)
					return
				} else {
					cid = c
					reader = bytes.NewReader(b)
				}
			} else {
				res.WriteHeader(415)
				return
			}

			if quads, graphs, err := styx.ParseMessage(reader); err != nil {
				res.WriteHeader(400)
			} else if r := db.HandleMessage(peerID.ID, cid, quads, graphs); res == nil {
				res.WriteHeader(204)
			} else {
				res.WriteHeader(200)

				encoder := json.NewEncoder(res)
				encoder.Encode(r)
			}
			return
		}
		fs.ServeHTTP(res, req)
	})

	log.Println("Listening on port 8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
