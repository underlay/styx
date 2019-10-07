package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	files "github.com/ipfs/go-ipfs-files"
	plugin "github.com/ipfs/go-ipfs/plugin"
	core "github.com/ipfs/interface-go-ipfs-core"
	ld "github.com/piprate/json-gold/ld"
	cbor "github.com/polydawn/refmt/cbor"

	styx "github.com/underlay/styx/db"
	loader "github.com/underlay/styx/loader"
)

// CborLdProtocol is the cbor-ld protocol string
const CborLdProtocol = "/ul/0.1.1/cbor-ld"

// NQuadsProtocol is the n-quads protocol string
const NQuadsProtocol = "/ul/0.1.1/n-quads"

// CborLdListenerPort is the cbor-ld listener port
const CborLdListenerPort = "4044"

// NQuadsListenerPort is the n-quads listener port
const NQuadsListenerPort = "4045"

// StyxPlugin is an IPFS deamon plugin
type StyxPlugin struct {
	host string
	lns  []net.Listener
	db   *styx.DB
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

func (sp *StyxPlugin) handleNQuadsConnection(conn net.Conn) {
	log.Println("Handling new n-quads connection", conn.LocalAddr())

	defer func() {
		log.Println("Closing n-quads connection", conn.LocalAddr())
		conn.Close()
	}()

	stringOptions := styx.GetStringOptions(sp.db.Loader)
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

		resolved, err := sp.db.API.Add(context.Background(), files.NewReaderFile(bytes.NewReader(b)))
		if err != nil {
			log.Println(err)
			continue
		}

		quads, graphs, err := styx.ParseMessage(bytes.NewReader(b))

		if response := sp.db.HandleMessage(resolved.Cid(), quads, graphs); response == nil {
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

	stringOptions := styx.GetStringOptions(sp.db.Loader)

	for {
		var doc map[string]interface{}
		err := unmarshaller.Unmarshal(&doc)
		if err != nil {
			log.Println(err)
			return
		}

		// Convert to RDF
		n, err := proc.Normalize(doc, stringOptions)
		if err != nil {
			log.Println(err)
			continue
		}

		normalized := n.(string)

		resolved, err := sp.db.API.Add(context.Background(), files.NewReaderFile(strings.NewReader(normalized)))
		if err != nil {
			log.Println(err)
			continue
		}

		log.Println("Received message", resolved.String())

		quads, graphs, err := styx.ParseMessage(strings.NewReader(normalized))
		if err != nil {
			log.Println(err)
			continue
		}

		if r := sp.db.HandleMessage(resolved.Cid(), quads, graphs); r != nil {
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
		url := fmt.Sprintf("%s/api/v0/p2p/listen?arg=%s&arg=%s&allow-custom-protocol=true", sp.host, protocol, address)
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
	path := os.Getenv("STYX_PATH")

	key, err := api.Key().Self(context.Background())
	if err != nil {
		return err
	}

	id := fmt.Sprintf("ul:/ipns/%s", key.ID().String())
	dl := loader.NewCoreDocumentLoader(api)
	sp.db, err = styx.OpenDB(path, id, dl, api.Unixfs())
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

	go log.Fatal(sp.db.Serve(styx.DefaultPort))

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
	host: "http://localhost:5001",
}}
