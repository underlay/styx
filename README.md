# styx

> Experimental graph store. Gateway to the Underworld.

Styx is like a key/value store for graph data. It takes RDF datasets in, and then you get data out with WHERE clauses where the pattern and result are expressed as RDF graphs.

## Interfaces

```golang
type Styx interface {
	Query(query []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (Cursor, error)
	Set(uri string, dataset []*ld.Quad) error
	Get(uri string) ([]*ld.Quad, error)
	Delete(uri string) error
	List(uri string) List
	Close() error
	Log()
}

type Iterator interface {
	Len() int
	Graph() []*ld.Quad
	Get(node *ld.BlankNode) ld.Node
	Domain() []*ld.BlankNode
	Index() []ld.Node
	Next(node *ld.BlankNode) ([]*ld.BlankNode, error)
	Seek(index []ld.Node) error
	Close()
}

type List interface {
	URI() string
	Next()
	Valid() bool
	Close()
}
```

## Usage

Typical usage will look something like this:

```golang
package main

import (
	styx "github.com/underlay/styx"
	ld "github.com/piprate/json-gold/ld"
)

var sampleData = []byte(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "Person",
	"name": "Johnanthan Appleseed",
	"knows": {
		"@id": "http://people.com/jane-doe"
	}
}`)

var sampleQuery = []byte(`{
	"@context": { "@vocab": "http://schema.org/" },
	"name": "Johnanthan Appleseed",
	"knows": { }
}`)


func main() {
	// Open a database at a path with a given URI scheme.
	// Passing an empty string for the path will open an in-memory instance
	tagScheme := styx.NewPrefixTagScheme("http://example.com/")
	db, _ := styx.OpenDB("/tmp/styx", tagScheme)
	defer db.Close()

	_ = db.SetJSONLD("http://example.com/d1", sampleData, false)

	iterator, err := db.Query()
	if err == styx.ErrEndOfSolutions {
		return
	}
	defer iterator.Close()
	for d := iterator.Domain(); err == nil; d, err = iterator.Next(nil) {
		for _, b := range d {
			fmt.Printf("%s: %s\n", b.Attribute, iterator.Get(b).GetValue())
		}
		fmt.Println("---")
	}
}
```
