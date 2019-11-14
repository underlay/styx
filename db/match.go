package db

import (
	"fmt"
	"log"
	"strconv"
	"time"

	multihash "github.com/multiformats/go-multihash"
	ld "github.com/piprate/json-gold/ld"

	query "github.com/underlay/styx/query"
)

var graphIri = ld.NewIRI("http://underlay.mit.edu/ns#Graph")
var queryIri = ld.NewIRI("http://underlay.mit.edu/ns#Query")

var memberIri = ld.NewIRI("http://www.w3.org/ns/ldp#member")

const valueIri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#value"
const indexIri = "http://underlay.mit.edu/ns#index"
const domainIri = "http://underlay.mit.edu/ns#domain"
const satisfiesIri = "http://underlay.mit.edu/ns#satisfies"
const enumeratesIri = "http://underlay.mit.edu/ns#enumerates"
const extentIri = "http://purl.org/dc/terms/extent"

const entityIri = "http://www.w3.org/ns/prov#Entity"

// const bundleIri = "http://www.w3.org/ns/prov#Bundle"

var provValueIri = ld.NewIRI("http://www.w3.org/ns/prov#value")

var rdfFirstIri = ld.NewIRI(ld.RDFFirst)
var rdfRestIri = ld.NewIRI(ld.RDFRest)
var rdfNilIri = ld.NewIRI(ld.RDFNil)

// Query is a type of query that can be executed
type Query interface {
	execute(
		label string, // label is the blank graph label of the top-level query
		graphs map[string][]int,
		quads []*ld.Quad,
		graph *ld.BlankNode,
		ds multihash.Multihash,
		db *DB,
	) []*ld.Quad
}

// # Instance Queries -----------------------------------------
type instanceQuery struct{}

func (q instanceQuery) execute(
	label string,
	graphs map[string][]int,
	quads []*ld.Quad,
	graph *ld.BlankNode,
	ds multihash.Multihash,
	db *DB,
) []*ld.Quad {
	variables := make(chan []string)
	data := make(chan []ld.Node)
	prov := make(chan query.Prov)
	go func() {
		if err := db.Query(quads, graphs[label], nil, nil, 1, variables, data, prov); err != nil {
			log.Println("hmm", err.Error())
		}
	}()
	v, _ := <-variables
	d, _ := <-data
	_, _ = <-prov
	if v != nil && d != nil && len(v) > 0 && len(d) == len(v) {
		variableMap := make(map[string]ld.Node, len(variables))
		for i, v := range v {
			variableMap[v] = d[i]
		}
		r := make([]*ld.Quad, len(graphs[label]))
		for i, j := range graphs[label] {
			q := quads[j]
			if blank, is := q.Subject.(*ld.BlankNode); is {
				if node, has := variableMap[blank.Attribute]; has {
					q.Subject = node
				} else {
					break
				}
			}
			if blank, is := q.Predicate.(*ld.BlankNode); is {
				if node, has := variableMap[blank.Attribute]; has {
					q.Predicate = node
				} else {
					break
				}
			}
			if blank, is := q.Object.(*ld.BlankNode); is {
				if node, has := variableMap[blank.Attribute]; has {
					q.Object = node
				} else {
					break
				}
			}
			q.Graph = graph
			r[i] = q
		}
		return r
	}
	return nil
}

var _ Query = (*instanceQuery)(nil)

// # Entity Queries -----------------------------------------

type entityQuery struct {
	target string
	extent int
	domain []string
	index  []ld.Node
}

func (q entityQuery) execute(
	label string,
	graphs map[string][]int,
	quads []*ld.Quad,
	graph *ld.BlankNode,
	ds multihash.Multihash,
	db *DB,
) []*ld.Quad {

	g := graph.Attribute

	id := ld.NewIRI(fmt.Sprintf("dweb:/ipns/%s", db.ID))
	entity := ld.NewBlankNode(fmt.Sprintf("%s-e", g))
	timeLiteral := ld.NewLiteral(time.Now().Format(time.RFC3339), xsdDateIri, "")
	uri := db.uri.String(ds, fmt.Sprintf("#%s", q.target))
	r := []*ld.Quad{
		ld.NewQuad(entity, typeIri, ld.NewIRI(entityIri), g),
		ld.NewQuad(entity, ld.NewIRI(satisfiesIri), ld.NewIRI(uri), g),
		ld.NewQuad(entity, wasAttributedToIri, id, g),
		ld.NewQuad(entity, generatedAtTimeIri, timeLiteral, g),
	}

	var value ld.Node = rdfNilIri
	var variables []string
	if h, v, t := handleEntity(graphs[q.target], q.domain, q.index, q.extent, quads, g, id, db); h != nil {
		r = append(r, t...)
		value = h
		variables = v
	} else {
		vars := make(chan []string)
		data := make(chan []ld.Node)
		prov := make(chan query.Prov)

		go func() {
			err := db.Query(quads, graphs[q.target], q.domain, q.index, q.extent, vars, data, prov)
			if err != nil {
				log.Println(err.Error())
			}
		}()

		v, ok := <-vars
		if !ok || v == nil {
			return nil
		}
		variables = v

		t := make([][]*ld.BlankNode, q.extent)
		for i := 0; i < q.extent; i++ {
			d, ok := <-data
			_ = <-prov
			if !ok || d == nil {
				break
			}
			t[i] = make([]*ld.BlankNode, len(d))
			var head ld.Node = rdfNilIri
			for j, node := range d {
				t[i][j] = ld.NewBlankNode(fmt.Sprintf("%s-t-%d-%d", g, i, j))
				r = append(r, ld.NewQuad(t[i][j], rdfFirstIri, node, g), ld.NewQuad(t[i][j], rdfRestIri, head, g))
				head = t[i][j]
			}
		}

		for i := len(t); i > 0; i-- {
			b := ld.NewBlankNode(fmt.Sprintf("%s-t-%d", g, i-1))
			var o ld.Node
			l := len(t[i-1])
			if l > 0 {
				o = t[i-1][l-1]
			} else {
				o = rdfNilIri
			}
			r = append(r, ld.NewQuad(b, rdfFirstIri, o, g), ld.NewQuad(b, rdfRestIri, value, g))
			value = b
		}
	}

	var domain ld.Node = rdfNilIri
	for i, v := range variables {
		b := ld.NewBlankNode(fmt.Sprintf("%s-d-%d", g, i))
		o := ld.NewIRI(db.uri.String(ds, fmt.Sprintf("#%s", v)))
		r = append(r, ld.NewQuad(b, rdfFirstIri, o, g), ld.NewQuad(b, rdfRestIri, domain, g))
		domain = b
	}

	r = append(
		r,
		ld.NewQuad(entity, ld.NewIRI(domainIri), domain, g),
		ld.NewQuad(entity, provValueIri, value, g),
	)

	return r
}

func matchQuery(label string, graphs map[string][]int, quads []*ld.Quad) Query {
	dataset := ld.NewRDFDataset()
	dataset.Graphs[label] = make([]*ld.Quad, len(graphs[label]))
	for i, x := range graphs[label] {
		dataset.Graphs[label][i] = quads[x]
	}

	api := ld.NewJsonLdApi()
	opts := ld.NewJsonLdOptions("")
	opts.UseNativeTypes = false

	if doc, err := api.FromRDF(dataset, opts); err != nil {
		return nil
	} else if len(doc) < 1 {
		return nil
	} else if first, is := doc[0].(map[string]interface{}); !is {
		return nil
	} else if array, is := first["@graph"].([]interface{}); !is {
		return nil
		// } else if entity := matchEntity(label, graphs, array); entity != nil {
		// 	return entity
	} else if entity := matchEntity(label, graphs, array); entity != nil {
		return entity
	} else {
		return &instanceQuery{}
	}
}

// // (I'm an idiot and don't know how to use Go)
// func matchEntity(label string, graphs map[string][]int, doc []interface{}) (entity *entityQuery) {
// 	if len(doc) != 1 {
// 		return
// 	} else if node, is := doc[0].(map[string]interface{}); !is {
// 		return
// 	} else if len(node) != 3 {
// 		return
// 	} else if types, is := node["@type"].([]interface{}); !is || len(types) != 1 || types[0] != entityIri {
// 		return
// 	} else if target := matchBlankNode(matchValue(node[satisfiesIri])); target == "" {
// 		return
// 	} else if _, has := graphs[target]; !has || target == label {
// 		return
// 	} else {
// 		return &entityQuery{target}
// 	}
// }

// (I'm really really dumb)
func matchEntity(label string, graphs map[string][]int, doc []interface{}) (entity *entityQuery) {
	if len(doc) != 1 {
		return
	} else if node, is := doc[0].(map[string]interface{}); !is {
		return
	} else if len(node) != 6 {
		return
	} else if nodeType := matchValue(node["@type"]); nodeType != entityIri {
		return
	} else if target := matchBlankNode(matchValue(node[satisfiesIri])); target == "" {
		return
	} else if _, has := graphs[target]; !has || target == label {
		return
	} else if extentNode, is := matchValue(node[extentIri]).(map[string]interface{}); !is || len(extentNode) != 2 {
		return
	} else if extentType, is := extentNode["@type"].(string); !is || extentType != ld.XSDInteger {
		return
	} else if extentValue, is := extentNode["@value"].(string); !is {
		return
	} else if extent, err := strconv.Atoi(extentValue); err != nil || extent < 0 {
		return
	} else if domainList := matchList(matchValue(node[domainIri])); domainList == nil {
		return
	} else if indexList := matchList(matchValue(node[indexIri])); indexList == nil {
		return
	} else if len(indexList) > len(domainList) {
		return
	} else {
		domain := make([]string, len(domainList))
		// Nodes in domainList must be blank nodes
		for i, node := range domainList {
			if domain[i] = matchBlankNode(node); domain[i] == "" {
				return
			}
		}

		index := make([]ld.Node, len(indexList))
		// Nodes in indexList must not be blank nodes
		for i, node := range indexList {
			if matchBlankNode(node) != "" {
				return
			} else if node, is := node.(map[string]interface{}); !is {
				return
			} else if iri, is := node["@id"].(string); is && len(node) == 1 {
				index[i] = ld.NewIRI(iri)
			} else if value, is := node["@value"].(string); is {
				if language, is := node["@language"].(string); is && len(node) == 2 {
					index[i] = ld.NewLiteral(value, ld.RDFLangString, language)
				} else if datatype, is := node["@type"].(string); is && len(value) == 2 {
					index[i] = ld.NewLiteral(value, datatype, "")
				} else if len(node) == 1 {
					index[i] = ld.NewLiteral(value, ld.XSDString, "")
				} else {
					return
				}
			} else {
				return
			}
		}

		return &entityQuery{target, extent, domain, index}
	}
}

func matchValue(value interface{}) interface{} {
	if array, is := value.([]interface{}); is && len(array) == 1 {
		return array[0]
	}
	return nil
}

func matchBlankNode(value interface{}) string {
	if value != nil {
		if node, is := value.(map[string]interface{}); is && len(node) == 1 {
			if id, is := node["@id"].(string); is && id[:2] == "_:" {
				return id
			}
		}
	}
	return ""
}

func matchList(value interface{}) []interface{} {
	if value != nil {
		if node, is := value.(map[string]interface{}); is && len(node) == 1 {
			if list, is := node["@list"].([]interface{}); is {
				return list
			}
		}
	}
	return nil
}

func handleEntity(
	graph []int,
	domain []string,
	index []ld.Node,
	extent int,
	quads []*ld.Quad,
	g string,
	id *ld.IRI,
	db *DB,
) (value ld.Node, variables []string, tail []*ld.Quad) {
	if len(graph) != 1 {
		return
	} else if q := quads[graph[0]]; !q.Predicate.Equal(memberIri) {
		return
	} else if iri, is := q.Subject.(*ld.IRI); !is || iri.Value != id.Value {
		return
	} else if blank, is := q.Object.(*ld.BlankNode); !is {
		return
	} else {
		pins := make(chan string)
		var mh multihash.Multihash = nil
		var fragment string
		if len(index) == 1 {
			if iri, is := index[0].(*ld.IRI); is {
				mh, fragment = db.uri.Parse(iri.Value)
				if fragment != "" {
					mh = nil
				}
			}
		}

		go func() {
			if err := db.Ls(mh, extent, pins); err != nil {
				fmt.Println("db.Ls failed:", err.Error())
			}
		}()

		results := make([]string, extent)

		i := 0
		for pin := range pins {
			results[i] = pin
			i++
		}

		tail := []*ld.Quad{}
		value = rdfNilIri
		for i := extent; i > 0; i-- {
			b := ld.NewBlankNode(fmt.Sprintf("%s-v-%d", g, i-1))
			next := ld.NewQuad(b, rdfRestIri, value, g)
			if results[i-1] == "" {
				tail = append(tail, ld.NewQuad(b, rdfFirstIri, rdfNilIri, g), next)
			} else {
				b0 := ld.NewBlankNode(fmt.Sprintf("%s-v-%d-0", g, i-1))
				tail = append(
					tail,
					ld.NewQuad(b, rdfFirstIri, b0, g),
					ld.NewQuad(b0, rdfFirstIri, ld.NewIRI(results[i-1]), g),
					ld.NewQuad(b0, rdfRestIri, rdfNilIri, g),
					next,
				)
			}
			value = b
		}

		return value, []string{blank.Attribute}, tail
	}
}
