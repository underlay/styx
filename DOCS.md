# Insertion

Styx handles six types of keys in twelve logical tables. The underlying key/value store [Badger](https://github.com/dgraph-io/badger) doesn't have a concept of a table, so instead we start every key with a prefix byte to indicate its type.

In this table, the 'p' that each key starts with is a single byte "prefix"
from the "prefixes" set. The prefix encodes both the key's type and its rotation.

| name    | #   | key format | value type | prefixes  |
| ------- | --- | ---------- | ---------- | --------- |
| triple  | 3   | `p a b c`  | SourceList | `{a b c}` |
| major   | 3   | `p a b`    | uint64     | `{i j k}` |
| minor   | 3   | `p a b`    | uint64     | `{x y z}` |
| value   | 1   | `p a`      | Value      | `{p}`     |
| index   | 1   | `p term`   | Index      | `{q}`     |
| graph   | 1   | `p cid`    | ISO Date   | `{g}`     |
| counter | 1   |            | uint64     | `{>}`     |

When inserting a graph:

- We check the graph key to see if the graph has been inserted before,
  and return without doing anything else if so. Otherwise, we write the
  current ISO date to the graph key.
- We look up each element's index key, if it exists.
  For each element, we either get a struct Index with a uint64 id, or we
  create a new one and write that to the index key. We also increment
  (or set to an initial 1) the Index.(position) counter: this is a count
  of the total number of times this id occurs in this position
  (.subject, .predicate, or .object) that we use a heuristic during
  query planning.
- We then insert the three triple keys. These are the rotations of the
  triple [a|s|p|o], [b|p|o|s], and [c|o|s|p], where s, p, and o are the
  uint64 ids we got from the index keys. The values for each of these
  keys are SourceList structs.
- Next we insert the three clockwise ("major") double keys with prefixes {ijk}
- Next we insert the three counter-clockwise ("minor") double keys with
  prefixes {xyz}

## Index keys

An Index keys starts with `q` and maps an RDF term (serialized per the [n-quads spec](https://www.w3.org/TR/n-quads/#n-quads-language)) to a protobuf-encoded `Index` struct containing the term's uint64 ID and three uint64 counters for the number of times it occurs in the database as a subject, predicate, and object.

## Value keys

A Value key starts with `p` and maps a (big-endian) uint64 ID to a protobuf-encoded `Value` struct that essentially mirrors the `ld.Node` iterface - representing one of a string `iri: string`, a struct `blank: Blank` with properties `Cid: []byte` and `id: string`, or a struct `literal: Literal` with properties `value: string` (required), `language: string` (optional), and `datatype: string` (optional).

## Triple keys

A triple key starts with one of `a`, `b`, or `c`, and their values are a protobuf-encoded list of Source structs containing the CID of the source message, and the integer index of a particular quad in the dataset.

_... more documentation to come ..._

---

# Querying

The response to a query has two components, both formatted as "RDF Maps", which are are [RDF Lists](https://www.w3.org/TR/rdf-schema/#ch_list) with one or more [`rdf:value`](https://www.w3.org/TR/rdf-schema/#ch_value) property on each of the list elements. All list elements are either blank nodes or IRIs, and they correspond to the concept of a "key" in the map. The objects of `rdf:value` are the "values" of the map, and there may be more than one for each key.

The first RDF Map contains the _value assignments_ for each of the blank nodes in the query. This means the list elements are the content-addressed URIs of the blank nodes in the query, and each of them have one or more IRI or Literal `rdf:value` values.

The second RDF Map contains the _provenance_ of query solution - in this case, the list elements are the _quads_ of the query graph, also referenced by content-addressed URI. The values of the map are the content-addressed URIs of one or more quads in previously-ingested datasets that instantiate that quad, and provide assignments for the blank nodes in them. In other words, the values are a list of "sources".

These maps are the `prov:value`s of two corresponding `prov:Entity` nodes that are related by a `prov:wasDerivedFrom` property (the value map's entity is the subject, and the provenance map's entity is the object).
These entity nodes are expliticly given an `rdf:type` of `prov:Entity`, and also carry a few additional properties:

- The value entity `ul:satisfies` the original query graph (by content-URI)
- The provenance entity has a `prov:wasAttributedTo` of the PeerId URI of the resolving node
- The provenance entity has a `prov:generatedAtTime` of the `xsd:dateTime` string of self-reported resolution time.

### Example

Suppose that we're a node with PeerId `QmYxMiLd4GXeW8FTSFGUiaY8imCksY6HH9LBq86gaFiwXG`, and we ingest a message...

```json
{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "Volcano",
	"name": "Mount Fuji",
	"smokingAllowed": true
}
```

... which has CID `QmPhqpDoDMCkQayAUFw2g1dtW8CxqYB8xVj4mRW8EUkcMf`.

Then we receive a query...

```json
{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "http://underlay.mit.edu/ns#:Query",
	"@graph": {
		"@type": "Volcano",
		"name": {},
		"smokingAllowed": {}
	}
}
```

All put together, our response to this query would look like this:

```json
{
	"@context": {
		"@vocab": "http://www.w3.org/ns/prov#",
		"q": "ul:/ipfs/QmPhqpDoDMCkQayAUFw2g1dtW8CxqYB8xVj4mRW8EUkcMf#/",
		"v": "ul:/ipfs/QmPhqpDoDMCkQayAUFw2g1dtW8CxqYB8xVj4mRW8EUkcMf#_:",
		"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
		"u": "http://underlay.mit.edu/ns#",
		"value": { "@container": "@list" },
		"xsd": "http://www.w3.org/2001/XMLSchema#"
	},
	"@graph": {
		"@type": "Entity",
		"u:satisfies": { "@id": "v:c14n2" },
		"value": [
			{
				"@id": "v:c14n0",
				"rdf:value": true
			},
			{
				"@id": "v:c14n1",
				"rdf:value": "Mount Fuji"
			},
			{
				"@id": "v:c14n3",
				"rdf:value": {
					"@id": "ul:/ipfs/QmNp2yo87y4nmXWvJgTGD3zurx7hKfDRiRWtRNPCvHC3rQ#_:c14n0"
				}
			}
		],
		"wasDerivedFrom": {
			"@type": "Entity",
			"generatedAtTime": {
				"@type": "xsd:dateTime",
				"@value": "2019-07-29T14:41:53-04:00"
			},
			"wasAttributedTo": {
				"@id": "ul:/ipns/QmYxMiLd4GXeW8FTSFGUiaY8imCksY6HH9LBq86gaFiwXG"
			},
			"value": [
				{
					"@id": "q:1",
					"rdf:value": "ul:/ipfs/QmNp2yo87y4nmXWvJgTGD3zurx7hKfDRiRWtRNPCvHC3rQ#/0"
				},
				{
					"@id": "q:2",
					"rdf:value": "ul:/ipfs/QmNp2yo87y4nmXWvJgTGD3zurx7hKfDRiRWtRNPCvHC3rQ#/1"
				},
				{
					"@id": "q:3",
					"rdf:value": "ul:/ipfs/QmNp2yo87y4nmXWvJgTGD3zurx7hKfDRiRWtRNPCvHC3rQ#/2"
				}
			]
		}
	}
}
```
