package main

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	content "github.com/joeltg/negotiate/content"
	ld "github.com/piprate/json-gold/ld"
	rdf "github.com/underlay/go-rdfjs"
	styx "github.com/underlay/styx"
)

type httpAPI struct {
	store *styx.Store
}

var jsonMime = "application/json"
var nQuadsMime = "application/n-quads"
var jsonLdMime = "application/ld+json"
var offers = []string{jsonMime, jsonLdMime, nQuadsMime}

func (api *httpAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var node rdf.Term = rdf.Default
	if r.URL.RawQuery != "" {
		_, err := url.Parse(r.URL.RawQuery)
		if err != nil {
			w.WriteHeader(400)
		}
		node = rdf.NewNamedNode(r.URL.RawQuery)
	}

	if r.Method == http.MethodGet {
		contentType := content.NegotiateContentType(r, offers, nQuadsMime)
		quads, err := api.store.Get(node)
		if err == styx.ErrNotFound {
			w.WriteHeader(404)
			return
		} else if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}

		if contentType == nQuadsMime {
			w.Header().Add("Content-Type", contentType)
			w.WriteHeader(200)
			for _, quad := range quads {
				w.Write([]byte(quad.String()))
				w.Write([]byte{'\n'})
			}
		} else if contentType == jsonLdMime {
			dataset := styx.ToRDFDataset(quads)
			opts := ld.NewJsonLdOptions(node.Value())
			opts.UseNativeTypes = true
			result, err := ld.NewJsonLdApi().FromRDF(dataset, opts)
			if err != nil {
				w.WriteHeader(500)
				w.Write([]byte(err.Error()))
				return
			}

			w.Header().Add("Content-Type", contentType)
			w.WriteHeader(200)
			_ = json.NewEncoder(w).Encode(result)
		} else if contentType == jsonMime {
			w.Header().Add("Content-Type", contentType)
			w.WriteHeader(200)
			_ = json.NewEncoder(w).Encode(quads)
		}
	} else if r.Method == http.MethodPut {
		contentType := r.Header.Get("Content-Type")
		if contentType != jsonLdMime && contentType != nQuadsMime && contentType != jsonMime {
			w.WriteHeader(415)
			return
		}

		if contentType == nQuadsMime {
			reader := bufio.NewReader(r.Body)
			var err error
			var line string
			quads := make([]*rdf.Quad, 0)
			for err != nil {
				line, err = reader.ReadString('\n')
				if line != "" {
					quads = append(quads, rdf.ParseQuad(line))
				}
			}
			if (err != nil && err != io.EOF) || len(quads) == 0 {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}

			err = api.store.Set(node, quads)
			if err != nil {
				w.WriteHeader(500)
				w.Write([]byte(err.Error()))
				return
			}

			w.WriteHeader(204)
		} else if contentType == jsonLdMime {
			var document interface{}
			err := json.NewDecoder(r.Body).Decode(&document)
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}

			err = api.store.SetJSONLD(node.Value(), document, false)
			if err != nil {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}
			w.WriteHeader(204)
		} else if contentType == jsonMime {
			var quads []*rdf.Quad
			err := json.NewDecoder(r.Body).Decode(&quads)
			if err != nil || len(quads) == 0 {
				w.WriteHeader(400)
				w.Write([]byte(err.Error()))
				return
			}

			err = api.store.Set(node, quads)
			if err != nil {
				w.WriteHeader(500)
				w.Write([]byte(err.Error()))
				return
			}

			w.WriteHeader(204)
		}
	} else if r.Method == http.MethodDelete {
		err := api.store.Delete(node)
		if err == styx.ErrNotFound {
			w.WriteHeader(404)
			return
		} else if err != nil {
			w.WriteHeader(500)
			w.Write([]byte(err.Error()))
			return
		}
		w.WriteHeader(204)
	} else {
		w.WriteHeader(405)
	}
}
