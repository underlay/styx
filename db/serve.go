package db

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	uuid "github.com/google/uuid"
	multihash "github.com/multiformats/go-multihash"
	ld "github.com/piprate/json-gold/ld"
	types "github.com/underlay/styx/types"
)

// DefaultPath for the Badger database files
const DefaultPath = "/tmp/styx"

// DefaultPort for the WebUI
const DefaultPort = "8086"

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
	api := ld.NewJsonLdApi()
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

			var mh multihash.Multihash
			var size uint32
			if m == "application/ld+json" {
				decoder := json.NewDecoder(req.Body)

				var doc interface{}
				if err := decoder.Decode(&doc); err != nil {
					res.WriteHeader(400)
					res.Write([]byte(err.Error() + "\n"))
					return
				}
				rdf, err := proc.Normalize(doc, options)
				if err != nil {
					res.WriteHeader(400)
					res.Write([]byte(err.Error() + "\n"))
					return
				}
				reader := strings.NewReader(rdf.(string))
				mh, err = db.Store.Put(reader)
				size = uint32(len(rdf.(string)))
			} else if m == "application/n-quads" {
				mh, err = db.Store.Put(req.Body)
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
					} else if mh, err := db.Store.Put(p); err != nil {
						res.WriteHeader(400)
						res.Write([]byte(err.Error() + "\n"))
						return
					} else {
						filesMap[base+name] = types.MakeFileURI(mh)
					}
				}

				walk(graph, filesMap)

				rdf, err := proc.Normalize(graph, options)
				if err != nil {
					res.WriteHeader(400)
					res.Write([]byte(err.Error() + "\n"))
					return
				}
				reader := strings.NewReader(rdf.(string))
				size = uint32(len(rdf.(string)))
				mh, err = db.Store.Put(reader)
			} else {
				res.WriteHeader(415)
				res.Write([]byte(err.Error() + "\n"))
				return
			}

			if err != nil {
				res.WriteHeader(500)
				res.Write([]byte(err.Error() + "\n"))
				return
			}

			var r *ld.RDFDataset
			if logging == "PROD" {
				r, err = db.HandleMessage(mh, size)
			} else {
				start := time.Now()
				r, err = db.HandleMessage(mh, size)
				log.Printf("Handled message in %s\n", time.Since(start))
			}

			if err != nil {
				res.WriteHeader(500)
				res.Write([]byte(err.Error() + "\n"))
			} else if r == nil {
				// cs := c.String()
				// res.Header().Add("Content-Type", "text/plain")
				// res.Header().Add("Location", fmt.Sprintf("/directory/?%s", cs))
				res.WriteHeader(201)
				// res.Write([]byte(cs))
			} else if normalized, err := api.Normalize(r, ld.NewJsonLdOptions("")); err != nil {
				res.WriteHeader(500)
				res.Write([]byte(err.Error() + "\n"))
			} else {
				res.Header().Add("Content-Type", "application/n-quads")
				res.WriteHeader(200)
				res.Write([]byte(normalized.(string)))
			}
			return
		}
		fs.ServeHTTP(res, req)
	})

	log.Printf("Serving %s/www at http://localhost:%s\n", db.Path, port)
	return http.ListenAndServe(":"+port, nil)
}
