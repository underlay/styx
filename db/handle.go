package db

import (
	"fmt"
	"log"
	"os"

	badger "github.com/dgraph-io/badger"
	cid "github.com/ipfs/go-cid"
	multihash "github.com/multiformats/go-multihash"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

var logging = os.Getenv("STYX_ENV")

var instanceOfIri = ld.NewIRI("http://underlay.mit.edu/ns#instanceOf")

// Context is the compaction context for CBOR-LD and HTTP
var Context = []byte(`{
	"@vocab": "http://www.w3.org/ns/prov#",
  "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	"xsd": "http://www.w3.org/2001/XMLSchema#",
	"u": "http://underlay.mit.edu/ns#",
	"u:instanceOf": { "@type": "@id" },
	"value": { "@container": "@list" },
	"generatedAtTime": { "@type": "xsd:dateTime" },
	"wasAttributedTo": { "@type": "@id" },
	"v": { "@id": "rdf:value", "@type": "@id" }
}`)

var typeIri = ld.NewIRI(ld.RDFType)

var derivedFromIri = ld.NewIRI("http://www.w3.org/ns/prov#wasDerivedFrom")
var generatedAtTimeIri = ld.NewIRI("http://www.w3.org/ns/prov#generatedAtTime")
var wasAttributedToIri = ld.NewIRI("http://www.w3.org/ns/prov#wasAttributedTo")

var xsdDateIri = "http://www.w3.org/2001/XMLSchema#date"

// HandleMessage is where all the magic happens.
func (db *DB) HandleMessage(mh multihash.Multihash, size uint32) (*ld.RDFDataset, error) {
	reader, err := db.Store.Get(mh)
	if err != nil {
		return nil, err
	}

	quads, graphs, err := ParseMessage(reader)
	if err != nil {
		return nil, err
	}

	fmt.Println("parsed some messages", graphs, err)

	if logging != "PROD" {
		c := cid.NewCidV1(cid.Raw, mh)
		log.Printf("Message: %s\n", c.String())
	}

	queries := map[string]bool{}
	data := map[string]chan map[string]*types.Value{}
	prov := map[string]chan map[int]*types.SourceList{}

	// Look through the default graph for graph names typed as queries
	for _, x := range graphs[""] {
		if s, is := quads[x].Subject.(*ld.BlankNode); is {
			if _, has := graphs[s.Attribute]; has {
				if quads[x].Predicate.Equal(typeIri) && quads[x].Object.Equal(queryIri) {
					if _, has := queries[s.Attribute]; !has {
						data[s.Attribute] = make(chan map[string]*types.Value)
						prov[s.Attribute] = make(chan map[int]*types.SourceList)
						queries[s.Attribute] = true
					}
				}
			}
		}
	}

	fmt.Println("queries", queries)

	// Messages are strictly either queries or data.
	// Any message that has a named graph typed to be a query in
	// the default graph will *not* have *any* of its graphs ingested.
	if len(queries) == 0 {
		err := db.Badger.Update(func(txn *badger.Txn) (err error) {
			valueMap := types.ValueMap{}

			graphList := make([]string, 0, len(graphs))
			for label, graph := range graphs {
				if len(graph) > 0 {
					graphList = append(graphList, label)
				}
			}

			var origin uint64
			origin, err = db.insertDataset(mh, uint32(len(quads)), uint32(size), graphList, valueMap, txn)
			if err != nil {
				return
			}

			indexMap := types.IndexMap{}
			for label, graph := range graphs {
				err = db.insertGraph(origin, quads, label, graph, indexMap, valueMap, txn)
				if err != nil {
					return
				}
			}

			err = indexMap.Commit(txn)
			if err != nil {
				return
			}

			err = valueMap.Commit(txn)
			return
		})

		if err != nil {
			log.Println("Error inserting dataset", err)
		}

		return nil, err
	}

	r := ld.NewRDFDataset()

	for label := range queries {
		graph := ld.NewBlankNode(label)
		if query := matchQuery(label, graphs, quads); query != nil {
			r.Graphs[label] = query.execute(label, graphs, quads, graph, mh, db)
			fmt.Println("got query", label, r.Graphs[label])
		}
		instance := ld.NewIRI(db.uri.String(mh, fmt.Sprintf("#%s", label)))
		r.Graphs["@default"] = append(r.Graphs["@default"], ld.NewQuad(graph, instanceOfIri, instance, ""))
	}

	return r, nil
}
