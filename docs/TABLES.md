# Tables and Data Layout

Styx handles six types of keys in twelve logical tables. The underlying key/value store [Badger](https://github.com/dgraph-io/badger) doesn't have a concept of a table, so instead we start every key with a single prefix byte to make virtual ones.

In this table, `l`, `m`, and `n` are all big-endian unsigned 64-bit integer identifiers.

| name    | #   | key format    | value type         | prefixes  |
| ------- | --- | ------------- | ------------------ | --------- |
| triple  | 3   | `l | m | n`   | `types.SourceList` | `{a b c}` |
| major   | 3   | `l | m`       | `uint64`           | `{i j k}` |
| minor   | 3   | `l | n`       | `uint64`           | `{x y z}` |
| value   | 1   | `l`           | `types.Value`      | `{p}`     |
| index   | 1   | `<term>`      | `types.Index`      | `{q}`     |
| graph   | 1   | `types.Blank` |                    | `{g}`     |
| counter | 1   |               | `uint64`           | `{>}`     |

- [Index table](#index-table)
- [Value table](#value-table)
- [Triple tables](#triple-tables)
- [Major and Minor tables](#major-and-minor-tables)
- [Graph table](#graph-table)

## Index table

```golang
const IndexPrefix = byte('q')
```

As a space-saving measure, every RDF term in every dataset that Styx ingests is given an unsigned 64-bit integer identifier. These assignments are kept in the Index table.

Keys in the Index table are RDF IRIs and Literals serialized as [N-Triples](https://www.w3.org/TR/n-triples/) terms. This means the IRIs are wrapped in chevrons, and the literals are wrapped in quotes and either have an explicit IRI datatype, an explicit language tag, or are assumed to have datatype `<http://www.w3.org/2001/XMLSchema#string>`.

Values in the Index table are serialized [Protobuf](https://developers.google.com/protocol-buffers/) structs that hold four unsigned 64-bit integers:

```protobuf
message Index {
  uint64 id = 1;
  uint64 subject = 2;
  uint64 predicate = 3;
  uint64 object = 4;
}
```

The `id` identifers are issued for new terms monotonically by a [Badger Sequence](https://godoc.org/github.com/dgraph-io/badger#Sequence). The `subject`, `predicate`, and `object` fields count the number of distinct quads in which the term appears in the respective position, which are use as hueristics during query resolution.

| key (as string)                                                        | value                                                 |
| ---------------------------------------------------------------------- | ----------------------------------------------------- |
| `q<http://xmlns.com/foaf/0.1/name>`                                    | `{ Id: 8234, Subject: 1, Predicate: 142, Object: 2 }` |
| `q"2011-04-09T20:00:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>` | `{ Id: 1129, Subject: 0, Predicate: 0, Object: 1 }`   |
| `q"N-Triples"@en-US"`                                                  | `{ Id: 18123, Subject: 0, Predicate: 0, Object: 2 }`  |

The Index table is the first table accessed during both insertion and querying. Both memoize deserialized structs (which include term identifiers) in an index map to deduplicate lookups.

```golang
type IndexMap map[string]*Index
```

### Querying

The first step in query processing is to retrieve the Index struct for every term in the query graph and store them in the index map. If any term is not found in the Index table, the query is rejected with no results, since the term does not exist in the database.

### Insertion

During insertion, for every term in every quad, either the existing Index struct is retrieved and incremented, or a new Index struct with a new identifier issued by the Sequence is instantiated. After every term is either created or incremented and the in-memory index map includes all the terms in the dataset, the index structs are re-serialized and written back to their corresponding keys.

## Value table

```golang
const ValuePrefix = byte('p')
```

The Value table inverts the Index table, mapping `uint64` keys to explicit RDF terms.

```protobuf
message Value {
  oneof node {
    string iri = 1;
    Blank blank = 2;
    Literal literal = 3;
  }
}

message Blank {
  bytes cid = 1;
  string id = 2;
}

message Literal {
  string value = 1;
  string language = 2;
  string datatype = 3;
}
```

Blank nodes _are_ given a separate Value type, even though they're semantically treated as content-addressed IRIs. This is only to get more compact CIDs by representing them directly as bytes in Protobuf instead of sacrificing a linear loss to base58 encoding.

| key (as bytes)             | value                                                                                                            |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `[112 0 0 0 0 0 0 32 42]`  | `iri "http://xmlns.com/foaf/0.1/name"`                                                                           |
| `[112 0 0 0 0 0 0 4 105]`  | `literal { value: "2011-04-09T20:00:00Z", datatype: "http://www.w3.org/2001/XMLSchema#dateTime", language: "" }` |
| `[112 0 0 0 0 0 0 70 203]` | `literal { value: "N-Triples", datatype: "http://www.w3.org/2000/01/rdf-schema#langString", language: "en-US" }` |

### Insertion

During insertion, only newly issued identifers for previously-unseen terms need to be written to the Value table.

### Querying

```golang
type ValueMap map[uint64]*Value
```

The variable assignments that the querying process returns are all uint64 identifiers, so they need to be converted into explicit RDF terms that can be returned to the user. The in-memory index map gets inverted to create an initial value map, which is used to memoize the Value table lookups for every term in the solution graph.

## Triple tables

```golang
var TriplePrefixes = [3]byte{'a', 'b', 'c'}
```

The most important tables that Styx maintains are the Triple tables. The three prefixes `{a b c}` correspond to the three rotations `{subject-predicate-object, predicate-object-subject, object-subject-predicate}` (respectively). The keys in each Triple table are exactly 25 bytes long: one for the appropriate prefix byte, and then three eight-byte big-endian uint64 ids that encode a triple in the appropriate order.

The first Triple table (`a: spo`) is further distinguished as the Source table. The `b` and `c` tables have empty values, but the keys in the `a` table carry protobuf-serialized lists of `Source` structs that hold the CID of the parent dataset, the graph label of the quad (or an empty string for the default graph), and the integer index of the quad in the canonicalized dataset (as sorted by the [URDNA2015](https://json-ld.github.io/normalization/spec/) algorithm).

```protobuf
message SourceList {
  repeated Source sources = 1;
}

message Source {
  bytes cid = 1;
  uint32 index = 2;
  string graph = 3;
}
```

| key (as bytes)                                         | value                          |
| ------------------------------------------------------ | ------------------------------ |
| `[97 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 2 0 0 0 0 0 0 0 3]` | `[{Qmfoo..., 13, "_:c14n0" }]` |
| `[98 0 0 0 0 0 0 0 2 0 0 0 0 0 0 0 3 0 0 0 0 0 0 0 1]` |                                |
| `[99 0 0 0 0 0 0 0 3 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 2]` |                                |

## Major and Minor tables

```golang
var MajorPrefixes = [3]byte{'i', 'j', 'k'}
var MinorPrefixes = [3]byte{'x', 'y', 'z'}
```

Styx keeps six more tables similar to the Triple tables, but with keys of just two terms: exactly 17 bytes each. The major table keys encode the three rotations of two terms (`{i: sp, j: po, k: os}`), and the minor table keys encode the three _reverse rotations_ of two terms (`{x: so, y: ps, z: op}`).

The values for all of these keys are (big-endian) uint64s that count the number of distinct entries in the corresponding Triple table that begin with the same two terms. The value for the key `[105 0 0 0 0 0 0 0 4 0 0 0 0 0 0 0 2]` is the _number of keys_ beginning with `[97 0 0 0 0 0 0 0 4 0 0 0 0 0 0 0 2]` (or, in other words, the number of distinct solutions for `[97 0 0 0 0 0 0 0 4 0 0 0 0 0 0 0 2 ? ? ? ? ? ? ? ?]`).

Every major key has a "dual" minor key that encodes the same two terms in reverse order. The prefixes of duals are offset by one: `i` keys are dual with `y` keys, `j` keys are dual with `z` keys, and `k` keys are dual with `x` keys. Every key has the same value as its dual.

| key (as bytes)                          | value |
| --------------------------------------- | ----- |
| `[105 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0 2]` | `5`   |
| `[121 0 0 0 0 0 0 0 2 0 0 0 0 0 0 0 1]` | `5`   |

## Graph table

```golang
const GraphPrefix = byte('g')
```

The Graph table lists every _graph_ in the database, with keys serialized directly as `Blank` structs re-used from the Value table. The values are empty, but may be used to store graph-level metadata in the future.
