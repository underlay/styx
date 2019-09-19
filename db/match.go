package db

import (
	"fmt"
	"strconv"

	ld "github.com/piprate/json-gold/ld"
)

var graphIri = ld.NewIRI("http://underlay.mit.edu/ns#Graph")
var queryIri = ld.NewIRI("http://underlay.mit.edu/ns#Query")

var indexIri = ld.NewIRI("http://underlay.mit.edu/ns#index")
var domainIri = ld.NewIRI("http://underlay.mit.edu/ns#domain")

var satisfiesIri = ld.NewIRI("http://underlay.mit.edu/ns#satisfies")
var enumeratesIri = ld.NewIRI("http://underlay.mit.edu/ns#enumerates")

var extentIri = ld.NewIRI("http://purl.org/dc/terms/extent")

var entityIri = ld.NewIRI("http://www.w3.org/ns/prov#Entity")
var bundleIri = ld.NewIRI("http://www.w3.org/ns/prov#Bundle")

func matchGraph(label string, graphs map[string][]int, quads []*ld.Quad) (
	entity, bundle bool,
	target string, extent int, domain map[string]ld.Node,
) {
	target, extent, domain = label, 1, map[string]ld.Node{}
	domainInclusion := map[string]bool{}

	var node string

	flags := [5]bool{false, false, false, false, false}

	for _, x := range graphs[label] {
		q := quads[x]

		if s, is := q.Subject.(*ld.BlankNode); is {
			if _, is := q.Object.(*ld.BlankNode); !is && q.Predicate.Equal(indexIri) {
				if _, has := domain[s.Attribute]; has {
					return
				}
				domain[s.Attribute] = q.Object
				continue
			} else if node != "" && node != s.Attribute {
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
			} else if q.Predicate.Equal(domainIri) {
				if o, is := q.Object.(*ld.BlankNode); is && o.Attribute != subject {
					domainInclusion[o.Attribute] = true
					continue
				}
			}
		}
		return
	}

	for v := range domain {
		if _, has := domainInclusion[v]; !has {
			return
		}
	}

	for v := range domainInclusion {
		if _, has := domain[v]; !has {
			domain[v] = nil
		}
	}

	entity, bundle = len(graphs[label]) == 2 && flags[0] && flags[1], flags[2] && flags[3]
	return
}

func matchBundle(graph []int, quads []*ld.Quad, t map[string]interface{}, entities []map[string]interface{}, db *DB) bool {
	extent := len(entities)
	q := quads[graph[0]]
	b, is := q.Subject.(*ld.BlankNode)
	if len(graph) == 1 && is && q.Predicate.Equal(typeIri) && q.Object.Equal(graphIri) {
		enumerator := fmt.Sprintf("q:%s", b.Attribute)
		messages := make(chan []byte, extent)
		dates := make(chan []byte, extent)
		go db.Ls("", extent, messages, dates)
		for x := 0; x < extent; x++ {
			entities[x] = map[string]interface{}{
				"@type":       "Entity",
				"u:satisfies": t,
			}
			if m, d := <-messages, <-dates; m == nil || d == nil {
				entities[x]["value"] = []interface{}{}
			} else {
				entities[x]["value"] = []interface{}{
					map[string]interface{}{
						"@id":          string(m),
						"u:instanceOf": enumerator,
						// "dateSubmitted": map[string]interface{}{
						// 	"@value": string(d),
						// 	"@type":  "xsd:dateTime",
						// },
					},
				}
			}
		}
		return true
	}
	return false
}
