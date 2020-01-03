# styx

> Experimental graph store. Gateway to the Underworld.

Styx is like a key/value store for graph data. It takes RDF datasets in, and then you get data out with WHERE clauses where the pattern and result are expressed as RDF graphs.

## Usage

The main module you want to import is `github.com/underlay/styx/db`, but the two interfaces you care about are defined in `github.com/underlay/styx/types`. Here they are:

```golang
package types

import (
	cid "github.com/ipfs/go-cid"
	ld "github.com/underlay/json-gold/ld"
)

type Styx interface {
	Query(query []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (Cursor, error)
	Insert(c cid.Cid, dataset []*ld.Quad) error
	Delete(c cid.Cid, dataset []*ld.Quad) error
	Close() error
	Log()
}

type Cursor interface {
	Graph() []*ld.Quad
	Get(node *ld.BlankNode) ld.Node
	Domain() []*ld.BlankNode
	Index() []ld.Node
	Next(node *ld.BlankNode) ([]*ld.BlankNode, error)
	Seek(index []ld.Node) error
	Close()
}
```

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

## Development

Regenerate the protobuf type definitions with:

```
protoc --go_out=. types/types.proto
```
