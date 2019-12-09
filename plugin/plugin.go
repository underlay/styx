package plugin

import (
	"context"
	"log"
	"os"

	plugin "github.com/ipfs/go-ipfs/plugin"
	core "github.com/ipfs/interface-go-ipfs-core"

	styx "github.com/underlay/styx/db"
)

// StyxPlugin is an IPFS deamon plugin
type StyxPlugin struct {
	db *styx.DB
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
	sp.db, err = styx.OpenDB(path, id, api)
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

	return nil
}

// Plugins is an exported list of plugins that will be loaded by go-ipfs.
var Plugins = []plugin.Plugin{&StyxPlugin{}}
