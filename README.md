# styx

> Experimental graph store. Gateway to the Underworld.

Styx is like a key/value store for graph data. It takes RDF datasets in, and then you get data out with WHERE clauses where the pattern and result are expressed as RDF graphs.

## Usage

Styx runs an HTTP interface at `http://localhost:8086` that you can interact with. It supports the following methods:

- `GET /` serves the WebUI built from `webui/www`.
- `POST /` with a `Content-Type` of either `application/ld+json` or `application/n-quads`. If the attached message contains queries, the response `200` body will contain a JSON-LD query response. Otherwise the route will respond `204` and persist the message to disk (and pin it to the local IPFS repo).
- `POST /` with a `Content-Type: multipart/form-data` **requires a query string** to indicate which of the named parts are the Underlay message. For example, POSTing a multipart form to `http://localhost:8000?foo` with a single JSON-LD part named `foo` is equivalent to POSTing the attachment directly to `http://localhost:8000`. Other attachments are treated as object blobs, which get pinned to IPFS. The resulting files can be referenced in the JSON-LD document with _relative_ `@id` URIs, with a relative path term of the part's name. For example, a part named `bar` can be referenced in the JSON-LD document with `"@id": "bar"`, which will get replaced with `"@id": "dweb:/ipfs/Qm..."` before getting translated into n-quads, hashed, and stored. _Note that the URI scheme for file objects is `dweb:`, not `ul:`._

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

## Roadmap

- Rules! We plan on implementing a variant of Datalog.
  - Linear Datalog with semi-naive evaluation would be simplest to implement
  - Handling of arithmetic / custom "evaluated" functions will be tricky
  - Datalog queries will need more elaborate provenance
- Reification
  - This will be how we implement provenance-based filtering (independent of Datlog or rules)
- Pinning
  - How to actually manage a styx node? What sorts of control mechanisms?
