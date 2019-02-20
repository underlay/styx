# Data Structures

## Insertion

Styx surrenders to five types of keys in eleven total tables. The underlying key-value store [Badger](https://github.com/dgraph-io/badger) doesn't have a concept of a table, so instead we prefix every key with two bytes to indicate its type.

### Index keys
An Index keys starts with `q` and maps an RDF term (serialized per the [n-quads spec](https://www.w3.org/TR/n-quads/#n-quads-language)) to a protobuf-encoded `Index` struct containing the term's uint64 ID and three uint64 counters for the number of times it occurs in the database as a subject, predicate, and object.

### Value keys
A Value key starts with `p` and maps a (big-endian) uint64 ID to a protobuf-encoded `Value` struct that essentially mirrors the `ld.Node` iterface - representing one of a string `iri: string`, a struct `blank: Blank` with properties `Cid: []byte` and `id: string`, or a struct `literal: Literal` with properties `value: string` (required), `language: string` (optional), and `datatype: string` (optional).

### Triple keys

A triple key starts with one of `a`, `b`, or `c`, depending on the  

## Querying

Although Styx operates on the RDF data model, it implements a subgraph matching algorithm that can be applied to any graph model that has a concept of "variables" with distinguishable but semantically insignificant labels. An abstract "variable in a graph" corresponds to an RDF "blank node in a dataset".

- Seek/Next/Value interface

Styx first transforms the pattern subgraph into an "undirected constraint graph" of **variable co-dependence**. Since we're biting the storage bullet to buy omni-directional indexing, triples that reference two blank nodes can be solved equally well in either order. This eliminates the directionality of each triple, reducing them to either 'static' constraints on a variable (in the case that there's only one variable in the triple, e.g. `_:person <http://schema.org/name> "Jane Doe"`) or 'dynamic' constraints between two variables (e.g. `_:jane <http://schema.org/knows> _:john .`). This happens in three steps: generating `Reference` structs from the dataset, and sorting the references into `Codex` structs, and gathering the codexes into a `CodexMap`.

The first step in query process is translating every triple in the pattern subgraph into zero to two References. A `Reference` struct represents an occurence of a variable in a graph - the RDF dataset

```
_:b0 <http://schema.org/name> "Jane Doe" .
_:b0 <http://schema.org/parent> _:b1 .
```

has two occurences of the variable `_:b0`, one occurence of the variable `_:b1`, and will produce three total references

| Variable | Index | Place | M                            | N                            |
| -------- | ----- | ----- | ---------------------------- | ---------------------------- |
| `_:b0`   | 0     | 0     | `<http://schema.org/name>`   | `"Jane Doe"`                 |
| `_:b0`   | 1     | 0     | `<http://schema.org/parent>` | `_:b1`                       |
| `_:b1`   | 1     | 2     | `_:b0`                       | `<http://schema.org/parent>` |

The fields of a `Reference` struct include the index of the source triple in the dataset, the "place" (0-2) of the variable within the triple, and M and N values for the next and previous (respectively and modularly) elements in the triple. In this example, the `_:b1` reference has place 2 (since `_:b2` appears as the object of the triple), M of `_:b0` (the "next" place modulo 3 is the subject), and N of the parent relation (the "previous" place modulo 3 is the predicate - which is also the "next next" place after the variable, and the "next" place after M).

The variable label isn't part of the reference struct: instead, all the references for a particular variable are collected into a "Codex" (named roughly "co-dependency" -> "co-deps" -> "codex") that sorts them by "degree".

- A single-degree reference is from a triple that only has one variable - both the reference's M and N are absolute values (IRIs or Literals in RDF), neither are other variables. The first reference in the example table is a _single_. Singles are stored in a flat slice `Single: []*Reference` in the codex.
- A double-degree reference is from triple that links two variables - the second and third references in the table are _doubles_. Double-degree references always occur in pairs (one from the perspective of each of the variables involved), so for convenience those references have a property `Dual: *Reference` with a pointer to its mirror. Doubles are stored in a map `Double: map[string][]*Reference` of a string label to a slice of references with that dual.
- There's a messy edge case when a triple references the same variable twice, so these references are categorized as a third degree _constraint_. Constraints are stored in a flat slice `Constraint: []*Reference`.

Each codex is in turn collected into a `CodexMap` that maps string variable labels to their codex.

(there are a few other properties throughout that are used as heuristics during query planning, but they're not architecturally important)

The final codex map for the example dataset encodes the undirected constraint graph of the variables in the query dataset.

```
CodexMap {
	_:b0: Codex {
		Single: [ Reference { 0 0 <http://schema.org/name> "Jane Doe" nil } ]
		Double: {
			_:b1: [ Reference {	1 0 <http://schema.org/parent> _:b1 *0xCAFE } ]
		}
		Constraint: []
	}
	_:b1: Codex {
		Single: []
		Double: {
			_:b0: [ Reference { 1 2 _:b0 <http://schema.org/parent> *0xBABE } ]
		}
		Constraint: []
	}
}
```

