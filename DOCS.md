# Data Structures

## Insertion

Styx surrenders to five types of keys in eleven total tables. The underlying key-value store [Badger](https://github.com/dgraph-io/badger) doesn't have a concept of a table, so instead we prefix every key with two bytes to indicate its type.

### Index keys

An Index keys starts with `q` and maps an RDF term (serialized per the [n-quads spec](https://www.w3.org/TR/n-quads/#n-quads-language)) to a protobuf-encoded `Index` struct containing the term's uint64 ID and three uint64 counters for the number of times it occurs in the database as a subject, predicate, and object.

### Value keys

A Value key starts with `p` and maps a (big-endian) uint64 ID to a protobuf-encoded `Value` struct that essentially mirrors the `ld.Node` iterface - representing one of a string `iri: string`, a struct `blank: Blank` with properties `Cid: []byte` and `id: string`, or a struct `literal: Literal` with properties `value: string` (required), `language: string` (optional), and `datatype: string` (optional).

### Triple keys

A triple key starts with one of `a`, `b`, or `c`.
