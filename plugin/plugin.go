package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"

	cid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	plugin "github.com/ipfs/go-ipfs/plugin"
	core "github.com/ipfs/interface-go-ipfs-core"
	ld "github.com/piprate/json-gold/ld"
	cbor "github.com/polydawn/refmt/cbor"

	"github.com/underlay/styx/db"
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

// Satisfies is the relation between a query and its result
const Satisfies = "http://underlay.mit.edu/ns#satisfies"

// StyxPlugin is an IPFS deamon plugin
type StyxPlugin struct {
	path           string
	api            string
	lns            []net.Listener
	db             *styx.DB
	documentLoader ld.DocumentLoader
	pinner         styx.DocumentStore
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

func (sp *StyxPlugin) handleMessage(dataset *ld.RDFDataset, cid cid.Cid) (response *ld.RDFDataset) {
	defaultGraph := dataset.Graphs[types.DefaultGraph]

	queries := map[string]chan []*ld.Quad{}

	for _, quad := range defaultGraph {
		if blankNode, isBlankNode := quad.Subject.(*ld.BlankNode); isBlankNode {
			if _, hasGraph := dataset.Graphs[blankNode.Attribute]; hasGraph {
				if iri, isIRI := quad.Predicate.(*ld.IRI); isIRI && iri.Value == ld.RDFType {
					if iri, isIRI := quad.Object.(*ld.IRI); isIRI && iri.Value == QueryType {
						queries[blankNode.Attribute] = make(chan []*ld.Quad)
					}
				}
			}
		}
	}

	for graph, quads := range dataset.Graphs {
		if graph != types.DefaultGraph {
			if _, isQuery := queries[graph]; isQuery {
				go sp.db.Query(quads, queries[graph])
			} else {
				go sp.db.IngestGraph(cid, graph, quads)
			}
		}
	}

	if len(queries) > 0 {
		hash := cid.String()
		response := ld.NewRDFDataset()
		for graph, result := range queries {
			if quads := <-result; quads != nil {
				response.Graphs[graph] = quads
				object := ld.NewIRI(fmt.Sprintf("ul:/ipfs/%s#%s", hash, graph))
				satisfies := ld.NewQuad(ld.NewBlankNode(graph), ld.NewIRI(Satisfies), object, types.DefaultGraph)
				response.Graphs[types.DefaultGraph] = append(response.Graphs[types.DefaultGraph], satisfies)
			}
		}
		if len(response.Graphs) > 1 {
			return response
		}
	}
	return nil
}

func (sp *StyxPlugin) handleNQuadsConnection(conn net.Conn) {
	serializer := &ld.NQuadRDFSerializer{}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)
	uvarint := make([]byte, 0, binary.MaxVarintLen64)
	for {
		m, err := binary.ReadUvarint(reader)
		if err != nil {
			return
		}

		bytes := make([]byte, m)
		n, err := io.ReadFull(reader, bytes)
		if err != nil {
			return
		} else if m != uint64(n) {
			return
		}

		dataset, err := serializer.Parse(bytes)
		if err != nil {
			log.Println(err)
			continue
		}

		cid, err := sp.pinner(bytes)
		if err != nil {
			log.Println(err)
			continue
		}

		response := sp.handleMessage(dataset, cid)
		if response != nil {
			res, err := serializer.Serialize(response)
			if err != nil {
				continue
			} else if serialized, isString := res.(string); !isString {
				continue
			} else {
				u := binary.PutUvarint(uvarint, uint64(len(serialized)))
				v, err := writer.Write(uvarint[:u])
				if err != nil || u != v {
					continue
				}
				w, err := writer.WriteString(serialized)
				if err != nil || w != len(serialized) {
					continue
				}
			}
		}
	}
}

func (sp *StyxPlugin) handleCborLdConnection(conn net.Conn) {
	marshaller := cbor.NewMarshaller(conn)
	unmarshaller := cbor.NewUnmarshaller(cbor.DecodeOptions{}, conn)
	proc := ld.NewJsonLdProcessor()

	datasetOptions := db.GetDatasetOptions(sp.db.Loader)
	stringOptions := db.GetStringOptions(sp.db.Loader)

	api := ld.NewJsonLdApi()

	defer conn.Close()

	for {
		var data map[string]interface{}
		err := unmarshaller.Unmarshal(&data)
		if err != nil {
			log.Println(err)
			return
		}

		// Convert to RDF
		rdf, err := proc.Normalize(data, datasetOptions)
		if err != nil {
			log.Println(err)
			continue
		}

		// Cast to *ld.RDFDataset
		dataset, isDataset := rdf.(*ld.RDFDataset)
		if !isDataset {
			continue
		}

		normalized, err := api.Normalize(dataset, stringOptions)
		if err != nil {
			log.Println(err)
			continue
		}

		cid, err := sp.pinner([]byte(normalized.(string)))
		if err != nil {
			log.Println(err)
			continue
		}

		response := sp.handleMessage(dataset, cid)
		if response != nil {
			json, _ := api.FromRDF(response, datasetOptions)
			compact, _ := proc.Compact(json, map[string]interface{}{}, datasetOptions)
			marshaller.Marshal(compact)
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
	sp.documentLoader = loader.NewCoreDocumentLoader(api)

	unixfsAPI := api.Unixfs()

	sp.pinner = func(nquads []byte) (cid.Cid, error) {
		file := files.NewBytesFile(nquads)
		path, err := unixfsAPI.Add(context.TODO(), file)
		if err != nil {
			return cid.Undef, err
		}
		return path.Cid(), nil
	}

	var err error

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
