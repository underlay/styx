package db

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

var logging = os.Getenv("STYX_ENV")

// Context is the compaction context for CBOR-LD
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

// HandleMessage is where all the magic happens.
func (db *DB) HandleMessage(
	c cid.Cid,
	quads []*ld.Quad,
	graphs map[string][]int,
) map[string]interface{} {

	if logging != "PROD" {
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

	// Messages are strictly either queries or data.
	// Any message that has a named graph typed to be a query in
	// the default graph will *not* have *any* of its graphs ingested.
	if len(queries) == 0 {
		for label, graph := range graphs {
			l, g := label, graph
			go func() {
				if err := db.Ingest(c, quads, l, g); err != nil {
					log.Println(err.Error())
				}
			}()
		}
		return nil
	}

	responses := make([]map[string]interface{}, 0, len(queries))

	for label := range queries {
		entity, bundle, target, extent, domain, index := matchGraph(label, graphs, quads)
		var graph interface{}
		t := map[string]interface{}{"@id": fmt.Sprintf("q:%s", target)}
		if entity {
			variables := make(chan []string)
			data := make(chan map[string]*types.Value)
			prov := make(chan map[int]*types.SourceList)
			l, g := label, graphs[target]
			go func() {
				if err := db.Query(quads, l, g, variables, data, prov); err != nil {
					log.Println(err.Error())
				}
			}()
			entity := makeEntity(t, <-variables, <-data, <-prov)
			entity["wasAttributedTo"] = fmt.Sprintf("ul:/ipns/%s", db.ID)
			entity["generatedAtTime"] = time.Now().Format(time.RFC3339)
			graph = entity
		} else if bundle {
			entities := make([]map[string]interface{}, extent)
			if !handleBundle(graphs[target], extent, domain, index, quads, t, entities, db) {
				variables := make(chan []string)
				data := make(chan map[string]*types.Value)
				prov := make(chan map[int]*types.SourceList)
				l, g := label, graphs[target]
				go func() {
					if err := db.Enumerate(quads, l, g, extent, domain, index, variables, data, prov); err != nil {
						log.Println(err.Error())
					}
				}()
				v := <-variables
				d := make([]map[string]*types.Value, extent)
				s := make([]map[int]*types.SourceList, extent)
				for x := range d {
					d[x] = <-data
				}

				for x := range s {
					s[x] = <-prov
				}

				for x := 0; x < extent; x++ {
					entities[x] = makeEntity(t, v, d[x], s[x])
				}
			}

			graph = map[string]interface{}{
				"@type":           "Bundle",
				"wasAttributedTo": fmt.Sprintf("ul:/ipns/%s", db.ID),
				"generatedAtTime": time.Now().Format(time.RFC3339),
				"dcterms:extent":  extent,
				"u:enumerates":    t,
				"value":           entities,
			}
		} else {
			variables := make(chan []string)
			data := make(chan map[string]*types.Value)
			prov := make(chan map[int]*types.SourceList)
			l, g := label, graphs[label]
			go func() {
				if err := db.Query(quads, l, g, variables, data, prov); err != nil {
					log.Println(err.Error())
				}
			}()
			_ = <-variables
			graph = makeGraph(graphs[label], quads, <-data)
			_ = <-prov
		}

		responses = append(responses, map[string]interface{}{
			"u:instanceOf": fmt.Sprintf("q:%s", label),
			"@graph":       graph,
		})
	}

	// Unmarshal context string
	context := map[string]interface{}{}
	if err := json.Unmarshal(Context, &context); err != nil {
		log.Println("Error unmarshalling context", err)
	}

	context["q"] = fmt.Sprintf("ul:/ipfs/%s#", c.String())

	return map[string]interface{}{
		"@context": context,
		"@graph":   responses,
	}
}

func makeEntity(
	target map[string]interface{},
	variables []string,
	d map[string]*types.Value,
	p map[int]*types.SourceList,
) map[string]interface{} {

	values := make([]interface{}, 0, len(d))
	entity := map[string]interface{}{
		"@type":       "Entity",
		"u:satisfies": target,
	}

	if len(d) == 0 || len(p) == 0 {
		entity["value"] = values
		return entity
	}

	for _, p := range variables {
		values = append(values, map[string]interface{}{
			"@id":       fmt.Sprintf("q:%s", p),
			"rdf:value": d[p].ToJSON(),
		})
	}

	entity["value"] = values

	var size int
	for _, sourceList := range p {
		size += len(sourceList.Sources)
	}

	sources := make([]map[string]interface{}, 0, size)
	for x, sourceList := range p {
		for _, source := range sourceList.Sources {
			sources = append(sources, map[string]interface{}{
				"@id":          source.GetValue(),
				"u:instanceOf": fmt.Sprintf("q:/%d", x),
			})
		}
	}

	entity["wasDerivedFrom"] = map[string]interface{}{
		"@type":     "Collection",
		"hadMember": sources,
	}

	return entity
}

func makeGraph(
	graph []int,
	quads []*ld.Quad,
	data map[string]*types.Value,
) []map[string]interface{} {
	nodes := map[string]map[string][]interface{}{}
	for _, x := range graph {
		var subject string
		if s, is := quads[x].Subject.(*ld.BlankNode); is {
			if value, has := data[s.Attribute]; has {
				if iri, is := value.Node.(*types.Value_Iri); is {
					subject = iri.Iri
				} else if blank, is := value.Node.(*types.Value_Blank); is {
					if c, err := cid.Cast(blank.Blank.Cid); err == nil {
						subject = fmt.Sprintf("ul:/ipfs/%s#%s", c.String(), blank.Blank.Id)
					}
				}
			}
		} else if s, is := quads[x].Subject.(*ld.IRI); is {
			subject = s.Value
		}

		if subject == "" {
			continue
		}

		var predicate string
		if p, is := quads[x].Predicate.(*ld.BlankNode); is {
			if value, has := data[p.Attribute]; has {
				if iri, is := value.Node.(*types.Value_Iri); is {
					predicate = iri.Iri
				}
			}
		} else if p, is := quads[x].Predicate.(*ld.IRI); is {
			predicate = p.Value
		}

		if predicate == "" {
			continue
		}

		var object interface{}
		if o, is := quads[x].Object.(*ld.BlankNode); is {
			if value, has := data[o.Attribute]; has {
				object = value.ToJSON()
			} else {
				continue
			}
		} else if o, is := quads[x].Object.(*ld.IRI); is {
			object = map[string]interface{}{"@id": o.Value}
		} else if o, is := quads[x].Object.(*ld.Literal); is {
			if o.Datatype == ld.XSDString {
				object = o.Value
			} else if o.Datatype == ld.XSDInteger {
				object, _ = strconv.Atoi(o.Value)
			} else if o.Datatype == ld.XSDDouble {
				object, _ = strconv.ParseFloat(o.Value, 64)
			} else if o.Datatype == ld.XSDBoolean {
				if o.Value == "true" {
					object = true
				} else if o.Value == "false" {
					object = false
				} else {
					object = map[string]interface{}{"@value": o.Value, "@type": o.Datatype}
				}
			} else if o.Datatype == ld.RDFLangString {
				object = map[string]interface{}{
					"@value":    o.Value,
					"@language": o.Language,
				}
			} else {
				object = map[string]interface{}{
					"@value": o.Value,
					"@type":  o.Datatype,
				}
			}
		}

		if node, has := nodes[subject]; has {
			if values, has := node[predicate]; has {
				node[predicate] = append(values, object)
			} else {
				node[predicate] = []interface{}{object}
			}
		} else {
			nodes[subject] = map[string][]interface{}{
				predicate: []interface{}{object},
			}
		}
	}
	doc := make([]map[string]interface{}, 0, len(nodes))
	for id, node := range nodes {
		n := make(map[string]interface{}, len(node)+1)
		n["@id"] = id
		for predicate, values := range node {
			if predicate == ld.RDFType {
				types := make([]string, len(values))
				for y, value := range values {
					if object, is := value.(map[string]interface{}); is {
						if id, has := object["@id"]; has && len(object) == 1 {
							types[y] = id.(string)
						}
					}
				}
				n["@type"] = types
			} else {
				n[predicate] = values
			}
		}
		doc = append(doc, n)
	}
	return doc
}
