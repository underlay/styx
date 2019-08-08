# styx

> Home-grown graph store inspired by [Hexastore](https://dl.acm.org/citation.cfm?id=1453965), tailored for use in the [Underlay](https://underlay.mit.edu).

Make sure your IPFS daemon is running, and start `./styx`.

## Usage

It registers a handler for the `/ul/0.1.1/cbor-ld` libp2p protocol so that connections dialed to your IPFS node's PeerId under that protocol will get forwarded to Styx. Underlay messages (RDF datasets expressing queries or data) can then be sent (serialized as cbor-encoded JSON-LD, or as utf-8 `application/n-quads`), and the Styx plugin will process them (storing the quads, pinning n-quads messages to IPFS, and responding to queries).

Styx runs an HTTP interface at `http://localhost:8000` that you can also interact with. It supports the following methods:
- `GET /` serves `www/index.html`, which is a query editor interface built on [Blockly](https://developers.google.com/blockly/).
- `POST /` with a `Content-Type` of either `application/ld+json` or `application/n-quads`. If the attached message contains queries, the response `200` body will contain a JSON-LD-serialized query response. Otherwise the route will respond `204` and persist the attached message to disk (and pin it to IPFS via HTTP API).
- `POST /` with a `Content-Type: multipart/form-data` **requires a query string** to indicate which of the named parts are the Underlay message. For example, POSTing a multipart form to `http://localhost:8000?foo` with a single JSON-LD part named `foo` is equivalent to POSTing the attachment directly to `http://localhost:8000`. Other attachments are treated as object blobs, which get pinned to IPFS. The resulting files can be referenced in the JSON-LD document with *relative* `@id` URIs, with a relative path term of the part's name. For example, an part named `bar` can be referenced in the JSON-LD document with `"@id": "bar"`, which will get replaced with `"@id": "dweb:/ipfs/Qm..."` before getting translated into n-quads, hashed, and stored. Note that the URI scheme for file objects is `dweb:`, not `ul:`.

Given a file `usb.pdf` and a file `usb.jsonld` containing:
```json
{
	"@context": {
		"@vocab": "http://schema.org/"
	},
	"@type": "DigitalDocument",
	"associatedMedia": {
		"@id": "usb",
		"encodingFormat": "application/pdf"
	}
}
```
You can POST both files to Styx using:
```
curl -F doc=@usb.jsonld -F usb=@usb.pdf http://localhost:8000?doc
```
... to result in Styx pinning `usb.pdf` to IPFS, and then translating the JSON-LD into:
```json
{
	"@context": {
		"@vocab": "http://schema.org/"
	},
	"@type": "DigitalDocument",
	"associatedMedia": {
		"@id": "dweb:/ipfs/QmaSKceV23ydYdD3wXL7vezsrmVc8kA1fcoowvuXULoELm",
		"encodingFormat": "application/pdf"
	}
}
```
... before processing the message as usual.

---

Another way to interact with styx is to also install [percolate](https://github.com/underlay/percolate) and run the [`examples/ping/index.js`](https://github.com/underlay/percolate/tree/master/examples/ping) example, which is configured to send a simple test message to the local IPFS node at `"/ip4/127.0.0.1/tcp/4001/ipfs/<YOUR-PEER-ID-HERE>"`.

## Roadmap

- Rules! We plan on implementing a variant of Datalog.
  - Linear Datalog with semi-naive evaluation would be simplest to implement
  - Handling of arithmetic / custom "evaluated" functions will be tricky
  - Datalog queries will need more elaborate provenance
- Reification
  - This will be how we implement provenance-based filtering (independent of Datlog or rules)
- Pinning
  - How to actually manage a styx node? What sorts of control mechanisms?
