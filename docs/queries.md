# Queries

Suppose that we're a node with PeerId `QmYxMiLd4GXeW8FTSFGUiaY8imCksY6HH9LBq86gaFiwXG`, and we ingest a message

```json
{
	"@context": {
		"@vocab": "http://schema.org/"
	},
	"@type": "Volcano",
	"name": "Mount Fuji",
	"smokingAllowed": true
}
```

... which normalizes to

```
_:c14n0 <http://schema.org/name> "Mount Fuji" .
_:c14n0 <http://schema.org/smokingAllowed> "true"^^<http://www.w3.org/2001/XMLSchema#boolean> .
_:c14n0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Volcano> .
```

... which has CID `QmNp2yo87y4nmXWvJgTGD3zurx7hKfDRiRWtRNPCvHC3rQ`. We'll call these IDs `ul:/ipns/...aFiwXG` and `ul:/ipfs/...vHC3rQ` for short.

## Simple Results

The simplest way to get data out of Styx is to send it a message like this:

```json
{
	"@context": {
		"@vocab": "http://schema.org/",
		"u": "http://underlay.mit.edu/ns#"
	},
	"@type": "u:Query",
	"@graph": {
		"@type": "Volcano",
		"name": {},
		"smokingAllowed": {}
	}
}
```

This message normalizes to

```
_:c14n2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://underlay.mit.edu/ns#Query> .
_:c14n3 <http://schema.org/name> _:c14n1 _:c14n2 .
_:c14n3 <http://schema.org/smokingAllowed> _:c14n0 _:c14n2 .
_:c14n3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Volcano> _:c14n2 .
```

... with CID `bafybeiaujfyny6f3fv3olxagtdohnesamofjigo7uwogp4au46mzlduxmi`, so we'll call it `ul:...lduxmi` for short.

The message that Styx will respond with might look like:

```json
{
	"@context": {
		"@vocab": "http://schema.org/",
		"u": "http://underlay.mit.edu/ns#"
	},
	"u:instanceOf": { "@id": "ul:...lduxmi#_:c14n2" },
	"@graph": {
		"@id": "ul:...lduxmi#_:c14n0",
		"@type": "Volcano",
		"name": "Mount Fuji",
		"smokingAllowed": true
	}
}
```

## Provenance

But suppose we want to know the _provenance_ of the result! For simple subgraph matching queries like the kind that Styx resolves, exhaustive provenance would just be a collection of "satisfying triples" from previous messages for each of the triples in the query graph. Provenance for more expressive queries that involve entailment, dynamic functions, arithmetic, etc. will be more complicated, but for now this is the most we're able to do.

To get the provenance of a query result, we _query for the provenance_ instead:

```json
{
	"@context": {
		"@vocab": "http://schema.org/",
		"prov": "http://www.w3.org/ns/prov#",
		"u": "http://underlay.mit.edu/ns#"
	},
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Entity",
		"u:satisfies": {
			"@graph": {
				"@type": "Volcano",
				"name": {},
				"smokingAllowed": {}
			}
		}
	}
}
```

This message normalizes to

```
_:c14n2 <http://underlay.mit.edu/ns#satisfies> _:c14n1 _:c14n3 .
_:c14n2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Entity> _:c14n3 .
_:c14n3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://underlay.mit.edu/ns#Query> .
_:c14n5 <http://schema.org/name> _:c14n4 _:c14n1 .
_:c14n5 <http://schema.org/smokingAllowed> _:c14n0 _:c14n1 .
_:c14n5 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Volcano> _:c14n1 .
```

... with CID `bafybeidym2ltjfu3f5yey3dxnrni73o45pwrkd2s6r5odfym3m4mum3b6m`, which we'll call `ul:...um3b6m` for short.

Here, the value of `u:satisfies` is a separate named graph, which is interpreted as a query graph (although it's important that it is not explicitly given an `rdf:type` of `u:Query`). But the value of `u:satisfies` could just have well have been `{ "@id": "ul:...lduxmi#_:c14n2" }` - a reference to the query graph of the previous message, so long as `...lduxmi` is resolvable over IPFS.

This time, Styx will respond with something like:

```json
{
	"@context": {
		"@vocab": "http://schema.org/",
		"xsd": "http://www.w3.org/2001/XMLSchema#",
		"u": "http://underlay.mit.edu/ns#",
		"u:instanceOf": { "@type": "@id" },
		"prov:value": { "@container": "@list" }
	},
	"u:instanceOf": "ul:/ipfs/...sWffFp#_:c14n5",
	"@graph": {
		"@type": "prov:Entity",
		"u:satisfies": { "@id": "ul:/ipfs/...sWffFp#_:c14n1" },
		"prov:value": [
			{
				"@id": "ul:/ipfs/...sWffFp#_:c14n7",
				"rdf:value": { "@id": "ul:/ipfs/...vHC3rQ#_:c14n0" }
			},
			{
				"@id": "ul:/ipfs/...sWffFp#_:c14n4",
				"rdf:value": "Mount Fuji"
			},
			{
				"@id": "ul:/ipfs/...sWffFp#_:c14n0",
				"rdf:value": true
			}
		],
		"prov:wasAttributedTo": { "@id": "ul:/ipns/...aFiwXG" },
		"prov:generatedAtTime": {
			"@value": "2019-08-15T18:23:55.129Z",
			"@type": "xsd:dateTime"
		},
		"prov:wasDerivedFrom": {
			"@type": "prov:Collection",
			"prov:hadMember": [
				{
					"@id": "ul:/ipfs/...vHC3rQ#/0",
					"u:instanceOf": "ul:/ipfs/...sWffFp#/5"
				},
				{
					"@id": "ul:/ipfs/...vHC3rQ#/1",
					"u:instanceOf": "ul:/ipfs/...sWffFp#/6"
				},
				{
					"@id": "ul:/ipfs/...vHC3rQ#/2",
					"u:instanceOf": "ul:/ipfs/...sWffFp#/7"
				}
			]
		}
	}
}
```

This repsonse has two major elements:

1. A `prov:Entity` node, which has a `prov:value` that contains the _value assignments_ for the query. The value of `prov:value` is an [RDF List](https://www.w3.org/TR/rdf-schema/#ch_list) whose elements are all of the variables in the query (identified by content-URI), along with exactly one additional `rdf:value` predicate linking the variable to its assigned value.
2. A `prov:Collection` node, which has `prov:hadMember` edges pointing to every RDF Statement that was used to generate the variable assignments in the `prov:Entity`. These RDF statements are identified by content-URI, and each have an additional `u:instanceOf` property linking them to the _triple in the query_ that they satisfy. It's likely that a triple in the query will end up getting satisfied by multiple statements from previous messages.

Given this structure, it's not immediately clear whether the `prov:Entity` or the `prov:Collection` should carry the `prov:wasAttributedTo` and `prov:generatedAtTime` properties. _Styx interprets these as properties of the `prov:Entity`, not their `prov:Collection` source._

Intuitively, the response expresses that "This ordered list of variables assignments is a `prov:Entity` that satisfies the query, and it was derived from this unordered collection of references to RDF Statements in previous messages."

## Multiple results

So far, we've seen Styx respond with exactly one set of satisfying values for a query. What if we want multiple values?

In the same way that we retrieved provenance by querying for it directly, we retrieve multiple result-sets by asking for them explitly, this time using a `prov:Bundle` wrapper and describing its size with [`dcterms:extent`](https://www.dublincore.org/specifications/dublin-core/dcmi-terms/2012-06-14/?v=terms#terms-extent). We also have to use a different predicate to describe our target pattern graph; for `prov:Bundle` nodes we use `u:enumerates` instead of `u:satisfies`.

```json
{
	"@context": {
		"@vocab": "http://schema.org/",
		"prov": "http://www.w3.org/ns/prov#",
		"dcterms": "http://purl.org/dc/terms/",
		"u": "http://underlay.mit.edu/ns#",
		"u:index": { "@container": "@list" }
	},
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Bundle",
		"dcterms:extent": 5,
		"u:index": [],
		"u:enumerates": {
			"@graph": {
				"@type": "Volcano",
				"name": {},
				"smokingAllowed": {}
			}
		}
	}
}
```

This message normalizes to

```
_:c14n2 <http://purl.org/dc/terms/extent> "5"^^<http://www.w3.org/2001/XMLSchema#integer> _:c14n4 .
_:c14n2 <http://underlay.mit.edu/ns#enumerates> _:c14n5 _:c14n4 .
_:c14n2 <http://underlay.mit.edu/ns#index> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil> _:c14n4 .
_:c14n2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Bundle> _:c14n4 .
_:c14n3 <http://schema.org/name> _:c14n1 _:c14n5 .
_:c14n3 <http://schema.org/smokingAllowed> _:c14n0 _:c14n5 .
_:c14n3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Volcano> _:c14n5 .
_:c14n4 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://underlay.mit.edu/ns#Query> .

```

.. with CID `QmQddvcxdKkch2viBgtY7ZqU5wrw1cFn4HbMf9DmccSqwk`, so we'll call it `ul:/ipfs/...ccSqwk` for short.

```json
{
	"@context": {
		"@vocab": "http://schema.org/",
		"xsd": "http://www.w3.org/2001/XMLSchema#",
		"u": "http://underlay.mit.edu/ns#",
		"u:instanceOf": { "@type": "@id" },
		"prov:value": { "@container": "@list" }
	},
	"u:instanceOf": "ul:/ipfs/...ccSqwk#_:c14n5",
	"@graph": {
		"@type": "prov:Bundle",
		"dcterms:extent": 5,
		"u:enumerates": { "@id": "ul:/ipfs/...ccSqwk#_:c14n8" },
		"prov:wasAttributedTo": { "@id": "ul:/ipns/...aFiwXG" },
		"prov:generatedAtTime": {
			"@value": "2019-08-15T18:23:55.129Z",
			"@type": "xsd:dateTime"
		},
		"prov:value": [
			{
				"@type": "prov:Entity",
				"prov:value": [
					{
						"@id": "ul:/ipfs/...ccSqwk#_:c14n6",
						"rdf:value": { "@id": "ul:/ipfs/...vHC3rQ#_:c14n0" }
					},
					{
						"@id": "ul:/ipfs/...ccSqwk#_:c14n2",
						"rdf:value": "Mount Fuji"
					},
					{
						"@id": "ul:/ipfs/...ccSqwk#_:c14n0",
						"rdf:value": true
					}
				],
				"prov:wasDerivedFrom": {
					"@type": "prov:Collection",
					"prov:hadMember": [
						{
							"@id": "ul:/ipfs/...vHC3rQ#/0",
							"u:instanceOf": "ul:/ipfs/...ccSqwk#/7"
						},
						{
							"@id": "ul:/ipfs/...vHC3rQ#/1",
							"u:instanceOf": "ul:/ipfs/...ccSqwk#/8"
						},
						{
							"@id": "ul:/ipfs/...vHC3rQ#/2",
							"u:instanceOf": "ul:/ipfs/...ccSqwk#/9"
						}
					]
				}
			},
			{ "@type": "prov:Entity", "prov:value": [] },
			{ "@type": "prov:Entity", "prov:value": [] },
			{ "@type": "prov:Entity", "prov:value": [] },
			{ "@type": "prov:Entity", "prov:value": [] }
		]
	}
}
```

In this case, the entries at index 1-4 of the `prov:Bundle`'s `prov:value` array have `prov:value` of `rdf:nil` because there were not enough results to populate them.
