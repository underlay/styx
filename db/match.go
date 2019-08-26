package db

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

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
var queryIri = ld.NewIRI("http://underlay.mit.edu/ns#Query")
var indexIri = ld.NewIRI("http://underlay.mit.edu/ns#index")
var enumeratesIri = ld.NewIRI("http://underlay.mit.edu/ns#enumerates")
var satisfiesIri = ld.NewIRI("http://underlay.mit.edu/ns#satisfies")
var extentIri = ld.NewIRI("http://purl.org/dc/terms/extent")
var entityIri = ld.NewIRI("http://www.w3.org/ns/prov#Entity")
var bundleIri = ld.NewIRI("http://www.w3.org/ns/prov#Bundle")
var valuePredicateIri = ld.NewIRI("http://www.w3.org/ns/prov#value")
var derivedFromIri = ld.NewIRI("http://www.w3.org/ns/prov#wasDerivedFrom")
var generatedAtTimeIri = ld.NewIRI("http://www.w3.org/ns/prov#generatedAtTime")
var wasAttributedToIri = ld.NewIRI("http://www.w3.org/ns/prov#wasAttributedTo")

// HandleMessage is where all the magic happens.
func (db *DB) HandleMessage(
	id string,
	cid cid.Cid,
	quads []*ld.Quad,
	graphs map[string][]int,
) map[string]interface{} {

	log.Println("Handling message", cid.String())

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
		for g, graph := range graphs {
			go db.Ingest(cid, quads, g, graph)
		}
		return nil
	}

	responses := make([]map[string]interface{}, 0, len(queries))

	for g := range queries {
		entity, bundle, target, extent, indices := matchGraph(g, graphs, quads)
		var graph interface{}
		t := map[string]interface{}{"@id": fmt.Sprintf("q:%s", target)}
		if entity {
			go db.Query(quads, g, graphs[target], data[g], prov[g])
			entity := makeEntity(t, <-data[g], <-prov[g])
			entity["prov:wasAttributedTo"] = fmt.Sprintf("ul:/ipns/%s", id)
			entity["prov:generatedAtTime"] = time.Now().Format(time.RFC3339)
			graph = entity
		} else if bundle {
			go db.Enumerate(quads, g, graphs[target], extent, indices, data[g], prov[g])
			d := make([]map[string]*types.Value, extent)
			s := make([]map[int]*types.SourceList, extent)
			for x := range d {
				d[x] = <-data[g]
			}
			for x := range s {
				s[x] = <-prov[g]
			}
			entities := make([]map[string]interface{}, extent)

			for x := 0; x < extent; x++ {
				entities[x] = makeEntity(t, d[x], s[x])
			}

			graph = map[string]interface{}{
				"@type":                "prov:Bundle",
				"prov:wasAttributedTo": fmt.Sprintf("ul:/ipns/%s", id),
				"prov:generatedAtTime": time.Now().Format(time.RFC3339),
				"dcterms:extent":       extent,
				"u:enumerates":         t,
				"prov:value":           entities,
			}
		} else {
			go db.Query(quads, g, graphs[target], data[g], prov[g])
			graph = makeGraph(graphs[target], quads, data[g])
		}
		responses = append(responses, map[string]interface{}{
			"u:instanceOf": fmt.Sprintf("q:%s", g),
			"@graph":       graph,
		})
	}

	// Unmarshal context string
	context := map[string]interface{}{}
	if err := json.Unmarshal(Context, &context); err != nil {
		log.Println("Error unmarshalling context", err)
	}

	context["q"] = fmt.Sprintf("ul:/ipfs/%s#", cid.String())

	return map[string]interface{}{
		"@context": context,
		"@graph":   responses,
	}
}

func matchGraph(label string, graphs map[string][]int, quads []*ld.Quad) (
	entity, bundle bool,
	target string, extent int, indices map[string]bool,
) {
	target, extent, indices = label, 1, map[string]bool{}
	var node string
	flags := [5]bool{false, false, false, false, false}
	for f, x := range graphs[label] {
		q := quads[x]
		if s, is := q.Subject.(*ld.BlankNode); is {
			if f > 0 && s.Attribute != node {
				return
			}

			node = s.Attribute

			if !flags[0] && q.Predicate.Equal(typeIri) && q.Object.Equal(entityIri) {
				flags[0] = true
				continue
			} else if !flags[1] && q.Predicate.Equal(satisfiesIri) {
				if o, is := q.Object.(*ld.BlankNode); is {
					if _, has := graphs[o.Attribute]; has {
						flags[1] = true
						target = o.Attribute
						continue
					}
				}
			} else if !flags[2] && q.Predicate.Equal(typeIri) && q.Object.Equal(bundleIri) {
				flags[2] = true
				continue
			} else if !flags[3] && q.Predicate.Equal(enumeratesIri) {
				if o, is := q.Object.(*ld.BlankNode); is {
					if _, has := graphs[o.Attribute]; has {
						flags[3] = true
						target = o.Attribute
						continue
					}
				}
			} else if !flags[4] && q.Predicate.Equal(extentIri) {
				if o, is := q.Object.(*ld.Literal); is && o.Datatype == ld.XSDInteger {
					flags[4] = true
					extent, _ = strconv.Atoi(o.Value)
					continue
				}
			} else if q.Predicate.Equal(indexIri) {
				if o, is := q.Object.(*ld.BlankNode); is && o.Attribute != subject {
					indices[o.Attribute] = true
					continue
				}
			}
		}
		return
	}
	entity, bundle = len(graphs[label]) == 2 && flags[0] && flags[1], flags[2] && flags[3]
	return
}

func makeEntity(
	target map[string]interface{},
	d map[string]*types.Value,
	p map[int]*types.SourceList,
) map[string]interface{} {

	values := make([]interface{}, 0, len(d))
	entity := map[string]interface{}{
		"@type":       "prov:Entity",
		"u:satisfies": target,
	}

	if len(d) == 0 || len(p) == 0 {
		entity["prov:value"] = values
		return entity
	}

	for p, value := range d {
		values = append(values, map[string]interface{}{
			"@id":       fmt.Sprintf("q:%s", p),
			"rdf:value": value.ToJSON(),
		})
	}

	entity["prov:value"] = values

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

	entity["prov:wasDerivedFrom"] = map[string]interface{}{
		"@type":          "prov:Collection",
		"prov:hadMember": sources,
	}

	return entity
}

func makeGraph(
	graph []int,
	quads []*ld.Quad,
	data chan map[string]*types.Value,
) []map[string]interface{} {
	d := <-data
	nodes := map[string]map[string][]interface{}{}
	for _, x := range graph {
		var subject string
		if s, is := quads[x].Subject.(*ld.BlankNode); is {
			if value, has := d[s.Attribute]; has {
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
			if value, has := d[p.Attribute]; has {
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
			if value, has := d[o.Attribute]; has {
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
					object = map[string]interface{}{"@value": o.Value, "@type": d}
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
