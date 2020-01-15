# styx

> Experimental graph store. Gateway to the Underworld.

Styx is like a key/value store for graph data. It takes RDF datasets in, and then you get data out with WHERE clauses where the pattern and result are expressed as RDF graphs.

## Usage

The main module you want to import is `github.com/underlay/styx/db`, but the interfaces you care about are defined in `github.com/underlay/styx/types`. Here they are:

```golang
type Styx interface {
	Query(query []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (Cursor, error)
	Insert(c cid.Cid, dataset []*ld.Quad) error
	Delete(c cid.Cid, dataset []*ld.Quad) error
	List(c cid.Cid) List
	Log()
	Close() error
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

type List interface {
	Cid() cid.Cid
	Next()
	Valid() bool
	Close()
}
```

Typical usage will look something like this:

```golang
package main

import (
	ipfs "github.com/ipfs/go-ipfs-http-client"
	styx "github.com/underlay/styx/db"
)

func main() {
	// You probably want a core.CoreAPI instance, either:
	// - an HTTP API from github.com/ipfs/go-ipfs-http-client
	// - a native CoreAPI from a IPFS daemon plugin
	httpAPI, _ := ipfs.NewURLApiWithClient("http://localhost:5001", http.DefaultClient)

	// Open a database at a path with a given URI scheme
	// passing nil will default to types.UnderlayURI, which formats URIs like
	// u:bafkreichbq6iklce3y64lntglbcw6grdmote5ptsxph4c5vm3j77br5coa#_:c14n0
	db, _ := styx.OpenDB("/tmp/styx", nil)
	defer db.Close()

	cursor, _ := db.Query()
	if cursor != nil {
		defer cursor.Close()
		for d := cursor.Domain(); d != nil; d, err = cursor.Next(nil) {
			for _, b := range d {
				fmt.Printf("%s: %s\n", b.Attribute, cursor.Get(b).GetValue())
			}
			fmt.Println("---")
		}
	}
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
