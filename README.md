# styx

> Home-grown graph store inspired by [Hexastore](https://dl.acm.org/citation.cfm?id=1453965), tailored for use in the [Underlay](https://underlay.mit.edu).

Make sure your IPFS daemon is running, and start `./styx`.

## Usage

It registers a handler for the `/ul/0.1.1/cbor-ld` libp2p protocol so that connections dialed to your IPFS node's PeerId under that protocol will get forwarded to Styx. Underlay messages (RDF datasets expressing queries or data) can then be sent (serialized as cbor-encoded JSON-LD, or as utf-8 `application/n-quads`), and the Styx plugin will process them (storing the quads and resolving query into response messages).

The simplest way to test it is to also install [percolate](https://github.com/underlay/percolate) and run the [`examples/ping/index.js`](https://github.com/underlay/percolate/tree/master/examples/ping) example, which is configured to send a simple test message to the local IPFS node at `"/ip4/127.0.0.1/tcp/4001/ipfs/<YOUR-PEER-ID-HERE>"`.
