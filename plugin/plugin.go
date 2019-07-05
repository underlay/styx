package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"

	cid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	plugin "github.com/ipfs/go-ipfs/plugin"
	core "github.com/ipfs/interface-go-ipfs-core"
	ld "github.com/piprate/json-gold/ld"
	cbor "github.com/polydawn/refmt/cbor"

	styx "github.com/underlay/styx/db"
	loader "github.com/underlay/styx/loader"
	types "github.com/underlay/styx/types"
)

// Protocol is the Underlay libp2p protocol string
const Protocol = "/ul/0.1.1/cbor-ld"

const listenPort = "4044"

// QueryType of queries in the Underlay
const QueryType = "http://underlay.mit.edu/ns#Query"

// Satisfies is the relation between a query and its result
const Satisfies = "http://underlay.mit.edu/ns#satisfies"

// StyxPlugin is an IPFS deamon plugin
type StyxPlugin struct {
	path           string
	api            string
	ln             net.Listener
	db             *styx.DB
	documentLoader ld.DocumentLoader
	pinner         func(normalized string) (cid.Cid, error)
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

// Init initializes plugin, satisfying the plugin.Plugin interface. Put any
// initialization logic here.
func (sp *StyxPlugin) Init() error {
	return nil
}

func (sp *StyxPlugin) handleConnection(conn net.Conn) {
	marshaller := cbor.NewMarshaller(conn)
	unmarshaller := cbor.NewUnmarshaller(cbor.DecodeOptions{}, conn)
	proc := ld.NewJsonLdProcessor()

	datasetOptions := ld.NewJsonLdOptions("")
	datasetOptions.ProcessingMode = ld.JsonLd_1_1
	datasetOptions.DocumentLoader = sp.db.Loader
	datasetOptions.UseNativeTypes = true
	datasetOptions.CompactArrays = true

	stringOptions := ld.NewJsonLdOptions("")
	stringOptions.ProcessingMode = ld.JsonLd_1_1
	stringOptions.DocumentLoader = sp.db.Loader
	stringOptions.Algorithm = types.Algorithm
	stringOptions.Format = types.Format

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

		cid, err := sp.pinner(normalized.(string))
		if err != nil {
			log.Println(err)
			continue
		}

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
					go sp.db.Query(graph, quads, queries[graph])
				} else {
					go sp.db.Ingest(cid, graph, quads)
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
				json, _ := api.FromRDF(response, datasetOptions)
				compact, _ := proc.Compact(json, map[string]interface{}{}, datasetOptions)
				marshaller.Marshal(compact)
			}
		}
	}
}

// Start gets passed a CoreAPI instance, satisfying the plugin.PluginDaemon interface.
func (sp *StyxPlugin) Start(api core.CoreAPI) error {
	sp.documentLoader = loader.NewCoreDocumentLoader(api)

	unixfsAPI := api.Unixfs()

	sp.pinner = func(normalized string) (cid.Cid, error) {
		file := files.NewBytesFile([]byte(normalized))
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

	sp.ln, err = net.Listen("tcp", fmt.Sprintf(":%s", listenPort))
	if err != nil {
		return err
	}

	address := "/ip4/127.0.0.1/tcp/" + listenPort

	go func() error {
		url := fmt.Sprintf("%s/api/v0/p2p/listen?arg=%s&arg=%s&allow-custom-protocol=true", sp.api, Protocol, address)
		res, err := http.Get(url)
		if err != nil {
			return err
		}

		log.Printf("Registering %s protocol handler: %s\n", Protocol, res.Status)

		if res.StatusCode != http.StatusOK {
			return nil
		}

		for {
			if conn, err := sp.ln.Accept(); err == nil {
				go sp.handleConnection(conn)
			} else {
				return err
			}
		}
	}()

	return nil
}

// Close gets called when the IPFS deamon shuts down, satisfying the plugin.PluginDaemon interface.
func (sp *StyxPlugin) Close() error {
	if sp.db != nil {
		if err := sp.db.Close(); err != nil {
			return err
		}
	}

	if sp.ln != nil {
		if err := sp.ln.Close(); err != nil {
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
