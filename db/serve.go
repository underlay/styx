package db

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strings"

	uuid "github.com/google/uuid"
	cid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	ld "github.com/piprate/json-gold/ld"
)

// Replace at your leisure
const DefaultPath = "/tmp/styx"
const DefaultPort = "8086"

var port = os.Getenv("STYX_PORT")

func walkValues(values []interface{}, files map[string]string) {
	for _, value := range values {
		if object, is := value.(map[string]interface{}); is {
			for key, val := range object {
				if id, is := val.(string); is && key == "@id" {
					if uri, has := files[id]; has {
						object["@id"] = uri
					}
				} else if array, is := val.([]interface{}); is && (key == "@list" || key == "@set") {
					walkValues(array, files)
				}
			}
		}
	}
}

func walk(graph []interface{}, files map[string]string) {
	for _, element := range graph {
		if node, is := element.(map[string]interface{}); is {
			for key, val := range node {
				if id, is := val.(string); is && key == "@id" {
					if uri, has := files[id]; has {
						node["@id"] = uri
					}
				} else if values, is := val.([]interface{}); is && key == "@graph" {
					walk(values, files)
				} else if is {
					walkValues(values, files)
				}
			}
		}
	}
}

// Serve attaches an HTTP endpoint
func (db *DB) Serve(port string) error {
	if port == "" {
		port = DefaultPort
	}

	proc := ld.NewJsonLdProcessor()
	options := GetStringOptions(db.Loader)

	dir := http.Dir(db.Path + "/www")

	fs := http.FileServer(dir)
	http.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		if req.Method == "POST" && req.URL.Path == "/" {
			ct := req.Header.Get("Content-Type")
			m, params, err := mime.ParseMediaType(ct)
			if err != nil {
				res.WriteHeader(400)
				res.Write([]byte(err.Error() + "\n"))
				return
			}

			var cid cid.Cid
			var reader io.Reader
			if m == "application/ld+json" {
				decoder := json.NewDecoder(req.Body)

				var doc interface{}
				if err := decoder.Decode(&doc); err != nil {
					res.WriteHeader(400)
					res.Write([]byte(err.Error() + "\n"))
					return
				}

				// Convert to RDF
				rdf, err := proc.Normalize(doc, options)
				if s, is := rdf.(string); !is || err != nil {
					res.WriteHeader(400)
					res.Write([]byte(err.Error() + "\n"))
					return
				} else if resolved, err := db.API.Add(context.Background(), files.NewReaderFile(strings.NewReader(s))); err != nil {
					res.WriteHeader(500)
					res.Write([]byte(err.Error() + "\n"))
					return
				} else {
					cid = resolved.Cid()
					reader = strings.NewReader(s)
				}
			} else if m == "application/n-quads" {
				if resolved, err := db.API.Add(context.Background(), files.NewReaderFile(req.Body)); err != nil {
					res.WriteHeader(400)
					res.Write([]byte(err.Error() + "\n"))
					return
				} else if node, err := db.API.Get(context.Background(), resolved); err != nil {
					res.WriteHeader(400)
					res.Write([]byte(err.Error() + "\n"))
					return
				} else if file, is := node.(files.File); is {
					reader = file
					cid = resolved.Cid()
				} else {
					res.WriteHeader(400)
					return
				}
			} else if boundary, has := params["boundary"]; m == "multipart/form-data" && has {
				r := multipart.NewReader(req.Body, boundary)
				filesMap := map[string]string{}
				u, err := uuid.NewRandom()
				if err != nil {
					res.WriteHeader(500)
					res.Write([]byte(err.Error() + "\n"))
					return
				}
				base := fmt.Sprintf("uuid://%s/", u.String())
				opts := ld.NewJsonLdOptions(base)
				opts.DocumentLoader = db.Loader
				opts.ProcessingMode = ld.JsonLd_1_1
				var graph []interface{}
				for {
					if p, err := r.NextPart(); err == io.EOF {
						break
					} else if err != nil {
						res.WriteHeader(400)
						res.Write([]byte(err.Error() + "\n"))
						return
					} else if name := p.FormName(); name == req.URL.RawQuery {
						if doc, err := ld.DocumentFromReader(p); err != nil {
							res.WriteHeader(400)
							res.Write([]byte(err.Error() + "\n"))
							return
						} else if expanded, err := proc.Expand(doc, opts); err != nil {
							res.WriteHeader(400)
							res.Write([]byte(err.Error() + "\n"))
							return
						} else if flattened, err := proc.Flatten(expanded, nil, opts); err != nil {
							res.WriteHeader(400)
							res.Write([]byte(err.Error() + "\n"))
							return
						} else {
							graph = flattened.([]interface{})
						}
					} else if resolved, err := db.API.Add(context.Background(), files.NewReaderFile(p)); err != nil {
						res.WriteHeader(400)
						res.Write([]byte(err.Error() + "\n"))
						return
					} else {
						id := base + name
						uri := fmt.Sprintf("dweb:/ipfs/%s", resolved.String())
						filesMap[id] = uri
					}
				}

				walk(graph, filesMap)

				// Convert to RDF
				rdf, err := proc.Normalize(graph, options)
				if s, is := rdf.(string); !is || err != nil {
					res.WriteHeader(400)
					res.Write([]byte(err.Error() + "\n"))
					return
				} else if resolved, err := db.API.Add(context.Background(), files.NewReaderFile(strings.NewReader(s))); err != nil {
					res.WriteHeader(500)
					res.Write([]byte(err.Error() + "\n"))
					return
				} else {
					cid = resolved.Cid()
					reader = strings.NewReader(s)
				}
			} else {
				res.WriteHeader(415)
				res.Write([]byte(err.Error() + "\n"))
				return
			}

			if quads, graphs, err := ParseMessage(reader); err != nil {
				res.WriteHeader(400)
				res.Write([]byte(err.Error() + "\n"))
			} else if r := db.HandleMessage(cid, quads, graphs); res == nil {
				res.WriteHeader(204)
			} else {
				res.Header().Add("Content-Type", "application/ld+json")
				res.WriteHeader(200)
				encoder := json.NewEncoder(res)
				encoder.Encode(r)
			}
			return
		}
		fs.ServeHTTP(res, req)
	})

	log.Printf("Serving %s/www at http://localhost:%s\n", db.Path, port)
	return http.ListenAndServe(":"+port, nil)
}
