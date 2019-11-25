package plugin

import (
	"context"
	"io"
	"log"
	"net"
	"os"

	cid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	plugin "github.com/ipfs/go-ipfs/plugin"
	core "github.com/ipfs/interface-go-ipfs-core"
	options "github.com/ipfs/interface-go-ipfs-core/options"
	path "github.com/ipfs/interface-go-ipfs-core/path"

	styx "github.com/underlay/styx/db"
	loader "github.com/underlay/styx/loader"
)

// CoreDocumentStore is a DocumentStore made from a core.BlockAPI
type CoreDocumentStore struct {
	api core.UnixfsAPI
}

// Put a block
func (unixFs *CoreDocumentStore) Put(reader io.Reader) (cid.Cid, error) {
	file := files.NewReaderFile(reader)
	resolved, err := unixFs.api.Add(context.Background(), file, options.Unixfs.RawLeaves(true), options.Unixfs.Pin(true))
	if err != nil {
		return cid.Undef, err
	}
	return resolved.Cid(), nil
}

// Get a block
func (unixFs *CoreDocumentStore) Get(c cid.Cid) (io.Reader, error) {
	node, err := unixFs.api.Get(context.Background(), path.IpfsPath(c))
	if file, is := node.(files.File); is {
		return file, err
	}
	return nil, err
}

var _ styx.DocumentStore = (*CoreDocumentStore)(nil)

// StyxPlugin is an IPFS deamon plugin
type StyxPlugin struct {
	host      string
	listeners []net.Listener
	db        *styx.DB
}

// Compile-time type check
var _ plugin.PluginDaemon = (*StyxPlugin)(nil)

// Name returns the plugin's name, satisfying the plugin.Plugin interface.
func (sp *StyxPlugin) Name() string {
	return "styx"
}

// Version returns the plugin's version, satisfying the plugin.Plugin interface.
func (sp *StyxPlugin) Version() string {
	return "0.2.0"
}

// Init initializes plugin, satisfying the plugin.Plugin interface.
func (sp *StyxPlugin) Init(env *plugin.Environment) error {
	return nil
}

// Start gets passed a CoreAPI instance, satisfying the plugin.PluginDaemon interface.
func (sp *StyxPlugin) Start(api core.CoreAPI) error {
	path := os.Getenv("STYX_PATH")
	port := os.Getenv("STYX_PORT")

	key, err := api.Key().Self(context.Background())
	if err != nil {
		return err
	}

	id := key.ID().String()
	dl := loader.NewCoreDocumentLoader(api)
	store := &CoreDocumentStore{api: api.Unixfs()}
	sp.db, err = styx.OpenDB(path, id, dl, store)
	if err != nil {
		return err
	}

	go func() {
		log.Fatal(sp.db.Serve(port))
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

	for _, ln := range sp.listeners {
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
