# styx

> Home-grown graph database inspired by [Hexastore](https://dl.acm.org/citation.cfm?id=1453965), tailored for use in the [Underlay](https://underlay.mit.edu).

Build the plugin with

```
make install
```

and then restart your (go-ipfs) IPFS daemon.

## Usage

Styx installs itself as an [IPFS daemon plugin](https://github.com/ipfs/go-ipfs/blob/master/docs/plugins.md#daemon) that will start and stop with the native IPFS daemon.

It registers a handler for the `/ul/0.1.1/cbor-ld` libp2p protocol so that connections dialed to your IPFS node's PeerId under that protocol will get forwarded to the Styx daemon. Underlay messages (RDF datasets expressing queries or data) can then be sent (serialized as cbor-encoded JSON-LD), and the Styx plugin will process them (ingesting the triples data messages, and resolving query messages and returning their results as response messages).

The simplest way to test it is to also install [percolate](https://github.com/underlay/percolate) and run the [`examples/ping/index.js`](https://github.com/underlay/percolate/tree/master/examples/ping) example, which is configured to send a simple test message to the local IPFS node at `"/ip4/127.0.0.1/tcp/4001/ipfs/<YOUR-PEER-ID-HERE>"`.
