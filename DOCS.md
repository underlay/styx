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
- Next we insert the three clockwise ("major") double keys with prefixes `{i j k}`
- Next we insert the three counter-clockwise ("minor") double keys with
  prefixes `{x y z}`

## Index keys

An Index keys starts with `q` and maps an RDF term (serialized per the [n-quads spec](https://www.w3.org/TR/n-quads/#n-quads-language)) to a protobuf-encoded `Index` struct containing the term's uint64 ID and three uint64 counters for the number of times it occurs in the database as a subject, predicate, and object.

## Value keys

A Value key starts with `p` and maps a (big-endian) uint64 ID to a protobuf-encoded `Value` struct that essentially mirrors the `ld.Node` iterface - representing one of a string `iri: string`, a struct `blank: Blank` with properties `Cid: []byte` and `id: string`, or a struct `literal: Literal` with properties `value: string` (required), `language: string` (optional), and `datatype: string` (optional).

## Triple keys

A triple key starts with one of `a`, `b`, or `c`, and their values are a protobuf-encoded list of Source structs containing the CID of the source message, and the integer index of a particular quad in the dataset.

_... more documentation to come ..._

---
