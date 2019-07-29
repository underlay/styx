package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	cid "github.com/ipfs/go-cid"
	plugin "github.com/ipfs/go-ipfs/plugin"
	core "github.com/ipfs/interface-go-ipfs-core"
	ld "github.com/piprate/json-gold/ld"
	cbor "github.com/polydawn/refmt/cbor"

	styx "github.com/underlay/styx/db"
	loader "github.com/underlay/styx/loader"
	types "github.com/underlay/styx/types"
)

// CborLdProtocol is the cbor-ld protocol string
const CborLdProtocol = "/ul/0.1.1/cbor-ld"

// NQuadsProtocol is the n-quads protocol string
const NQuadsProtocol = "/ul/0.1.1/n-quads"

// CborLdListenerPort is the cbor-ld listener port
const CborLdListenerPort = "4044"

// NQuadsListenerPort is the n-quads listener port
const NQuadsListenerPort = "4045"

// QueryType of queries in the Underlay
const QueryType = "http://underlay.mit.edu/ns#Query"

// Context is the compaction context for CBOR-LD
var Context = []byte(`{
	"@vocab": "http://www.w3.org/ns/prov#",
	"value": { "@container": "@list" },
	"u": "http://underlay.mit.edu/ns#",
	"xsd": "http://www.w3.org/2001/XMLSchema#",
  "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	"v": { "@id": "rdf:value", "@type": "@id" }
}`)

// StyxPlugin is an IPFS deamon plugin
type StyxPlugin struct {
	id     string
	path   string
	api    string
	lns    []net.Listener
	db     *styx.DB
	loader ld.DocumentLoader
	store  styx.DocumentStore
}

// Compile-time type check (bleh)
var _ plugin.PluginDaemon = (*StyxPlugin)(nil)

// Name returns the plugin's name, satisfying the plugin.Plugin interface.
func (sp *StyxPlugin) Name() string {
	return "styx"
}

// Version returns the plugin's version, satisfying the plugin.Plugin interface.
func (sp *StyxPlugin) Version() string {
	return "0.1.0"
}

// Init initializes plugin, satisfying the plugin.Plugin interface.
func (sp *StyxPlugin) Init() error {
	return nil
}

func (sp *StyxPlugin) handleMessage(cid cid.Cid, quads []*ld.Quad, graphs map[string][]int) map[string]interface{} {
	queries := map[string][]int{}
	data := map[string]chan map[string]*types.Value{}
	prov := map[string]chan map[int]*types.SourceList{}
	for _, quad := range quads {
		if quad.Graph != nil {
			continue
		}

		if b, is := quad.Subject.(*ld.BlankNode); is {
			if indices, has := graphs[b.Attribute]; has {
				if iri, isIRI := quad.Predicate.(*ld.IRI); isIRI && iri.Value == ld.RDFType {
					if iri, isIRI := quad.Object.(*ld.IRI); isIRI && iri.Value == QueryType {
						data[b.Attribute] = make(chan map[string]*types.Value)
						prov[b.Attribute] = make(chan map[int]*types.SourceList)
						queries[b.Attribute] = indices
					}
				}
			}
		}
	}

	// Messages are strictly either queries or data.
	// Any message that has a named graph typed to be a query in
	// the default graph will *not* have *any* of its graphs ingested.
	if len(queries) > 0 {
		for graph, indices := range queries {
			go sp.db.Query(quads, graph, indices, data[graph], prov[graph])
		}
	} else {
		for graph, indices := range graphs {
			go sp.db.Ingest(cid, quads, graph, indices)
		}
	}

	if len(queries) > 0 {
		hash := cid.String()

		g := make([]map[string]interface{}, 0, len(queries))

		for graph := range queries {
			q := map[string]interface{}{
				"@type":       "Entity",
				"u:satisfies": map[string]interface{}{"@id": fmt.Sprintf("qv:%s", graph[2:])},
			}

			d, p := <-data[graph], <-prov[graph]
			if len(d) > 0 && len(p) > 0 {
				vl := make([]map[string]interface{}, 0, len(d))

				for label, value := range d {
					vl = append(vl, map[string]interface{}{
						"@id":       fmt.Sprintf("qv:%s", label[2:]),
						"rdf:value": value.ToJSON(),
					})
				}

				q["value"] = vl

				pl := make([]map[string]interface{}, 0, len(p))
				for index, sources := range p {
					values := make([]string, len(sources.Sources))
					for i, source := range sources.Sources {
						values[i] = source.GetValue()
					}
					pl = append(pl, map[string]interface{}{
						"@id": fmt.Sprintf("qp:%d", index),
						"v":   values,
					})
				}

				q["wasDerivedFrom"] = map[string]interface{}{
					"@type": "Entity",
					"generatedAtTime": map[string]interface{}{
						"@type":  "xsd:dateTime",
						"@value": time.Now().Format(time.RFC3339),
					},
					"wasAttributedTo": map[string]interface{}{"@id": sp.id},
					"value":           pl,
				}
			} else {
				q["value"] = []interface{}{}
			}

			g = append(g, q)
		}

		// Unmarshal context string
		context := map[string]interface{}{}
		if err := json.Unmarshal(Context, &context); err != nil {
			log.Println("Error unmarshalling context", err)
		}
		context["qv"] = fmt.Sprintf("ul:/ipfs/%s#_:", hash)
		context["qp"] = fmt.Sprintf("ul:/ipfs/%s#/", hash)

		doc := map[string]interface{}{
			"@context": context,
			"@graph":   g,
		}

		return doc
	}

	return nil
}

func (sp *StyxPlugin) handleNQuadsConnection(conn net.Conn) {
	log.Println("Handling new n-quads connection", conn.LocalAddr())

	defer func() {
		log.Println("Closing n-quads connection", conn.LocalAddr())
		conn.Close()
	}()

	stringOptions := styx.GetStringOptions(sp.loader)
	proc := ld.NewJsonLdProcessor()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	uvarint := make([]byte, 0, binary.MaxVarintLen64)
	for {
		m, err := binary.ReadUvarint(reader)
		if err != nil {
			return
		}

		b := make([]byte, m)
		n, err := io.ReadFull(reader, b)
		if err != nil {
			return
		} else if m != uint64(n) {
			return
		}

		cid, err := sp.store(bytes.NewReader(b))
		if err != nil {
			log.Println(err)
			continue
		}

		quads, graphs, err := styx.ParseMessage(bytes.NewReader(b))

		if response := sp.handleMessage(cid, quads, graphs); response == nil {
			continue
		} else if res, err := proc.ToRDF(response, stringOptions); err != nil {
			continue
		} else if serialized, is := res.(string); !is {
			continue
		} else {
			u := binary.PutUvarint(uvarint, uint64(len(serialized)))
			if v, err := writer.Write(uvarint[:u]); err != nil || u != v {
				continue
			} else if w, err := writer.WriteString(serialized); err != nil || w != len(serialized) {
				continue
			}
		}
	}
}

func (sp *StyxPlugin) handleCborLdConnection(conn net.Conn) {
	log.Println("Handling new cbor-ld connection", conn.LocalAddr())
	defer func() {
		log.Println("Closing cbor-ld connection", conn.LocalAddr())
		conn.Close()
	}()

	marshaller := cbor.NewMarshaller(conn)
	unmarshaller := cbor.NewUnmarshaller(cbor.DecodeOptions{}, conn)
	proc := ld.NewJsonLdProcessor()

	stringOptions := styx.GetStringOptions(sp.loader)

	for {
		var data map[string]interface{}
		err := unmarshaller.Unmarshal(&data)
		if err != nil {
			log.Println(err)
			return
		}

		// Convert to RDF
		normalized, err := proc.Normalize(data, stringOptions)
		if err != nil {
			log.Println(err)
			continue
		}

		cid, err := sp.store(bytes.NewReader([]byte(normalized.(string))))
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("Received message", cid.String())

		quads, graphs, err := styx.ParseMessage(strings.NewReader(normalized.(string)))
		if err != nil {
			log.Println(err)
			continue
		}

		if r := sp.handleMessage(cid, quads, graphs); r != nil {
			marshaller.Marshal(r)
		}
	}
}

func (sp *StyxPlugin) attach(port string, protocol string, handler func(conn net.Conn)) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return err
	}

	sp.lns = append(sp.lns, ln)

	address := "/ip4/127.0.0.1/tcp/" + port
	go func() error {
		url := fmt.Sprintf("%s/api/v0/p2p/listen?arg=%s&arg=%s&allow-custom-protocol=true", sp.api, protocol, address)
		res, err := http.Get(url)
		if err != nil {
			return err
		}

		log.Printf("Registering %s protocol handler: %s\n", protocol, res.Status)

		if res.StatusCode != http.StatusOK {
			return nil
		}

		for {
			if conn, err := ln.Accept(); err == nil {
				go handler(conn)
			} else {
				return err
			}
		}
	}()

	return nil
}

// Start gets passed a CoreAPI instance, satisfying the plugin.PluginDaemon interface.
func (sp *StyxPlugin) Start(api core.CoreAPI) error {
	sp.loader = loader.NewCoreDocumentLoader(api)
	sp.store = styx.MakeAPIDocumentStore(api.Unixfs())

	key, err := api.Key().Self(context.Background())
	if err != nil {
		return err
	}

	sp.id = fmt.Sprintf("ul:/ipns/%s", key.ID().String())

	sp.db, err = styx.OpenDB(sp.path)
	if err != nil {
		return err
	}

	err = sp.attach(CborLdListenerPort, CborLdProtocol, sp.handleCborLdConnection)
	if err != nil {
		return err
	}

	err = sp.attach(NQuadsListenerPort, NQuadsProtocol, sp.handleNQuadsConnection)
	if err != nil {
		return err
	}

	return nil
}

// Close gets called when the IPFS deamon shuts down, satisfying the plugin.PluginDaemon interface.
func (sp *StyxPlugin) Close() error {
	if sp.db != nil {
		if err := sp.db.Close(); err != nil {
			return err
		}
	}

	for _, ln := range sp.lns {
		if err := ln.Close(); err != nil {
			return err
		}
	}

	return nil
}

// Plugins is an exported list of plugins that will be loaded by go-ipfs.
var Plugins = []plugin.Plugin{&StyxPlugin{
	path: "/tmp/badger",
	api:  "http://localhost:5001",
}}
