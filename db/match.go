package db

import (
	"fmt"
	"log"
	"strconv"

	ld "github.com/piprate/json-gold/ld"
)

var graphIri = ld.NewIRI("http://underlay.mit.edu/ns#Graph")
var queryIri = ld.NewIRI("http://underlay.mit.edu/ns#Query")

const valueIri = "http://www.w3.org/1999/02/22-rdf-syntax-ns#value"
const indexIri = "http://underlay.mit.edu/ns#index"
const satisfiesIri = "http://underlay.mit.edu/ns#satisfies"
const enumeratesIri = "http://underlay.mit.edu/ns#enumerates"
const extentIri = "http://purl.org/dc/terms/extent"

const entityIri = "http://www.w3.org/ns/prov#Entity"
const bundleIri = "http://www.w3.org/ns/prov#Bundle"

func matchGraph(label string, graphs map[string][]int, quads []*ld.Quad) (
	entity, bundle bool,
	target string, extent int, domain []string, index []ld.Node,
) {
	dataset := ld.NewRDFDataset()
	dataset.Graphs[label] = make([]*ld.Quad, len(graphs[label]))
	for i, x := range graphs[label] {
		dataset.Graphs[label][i] = quads[x]
	}

	api := ld.NewJsonLdApi()
	opts := ld.NewJsonLdOptions("")
	opts.UseNativeTypes = false

	if doc, err := api.FromRDF(dataset, opts); err != nil {
		return
	} else if len(doc) < 1 {
		return
	} else if first, is := doc[0].(map[string]interface{}); !is {
		return
	} else if array, is := first["@graph"].([]interface{}); !is {
		return
	} else if target, entity = matchEntity(array); entity {
		return
	} else if target, extent, domain, index, bundle = matchBundle(array); bundle {
		return
	}

	return
}

// (I'm an idiot and don't know how to use Go)
func matchEntity(doc []interface{}) (target string, isEntity bool) {
	if len(doc) != 1 {
		return
	} else if node, is := doc[0].(map[string]interface{}); !is {
		return
	} else if len(node) != 3 {
		return
	} else if types, is := node["@type"].([]interface{}); !is || len(types) != 1 {
		return
	} else if types[0] != entityIri {
		return
	} else if satisfiesArray, is := node[satisfiesIri].([]interface{}); !is || len(satisfiesArray) != 1 {
		return
	} else {
		return matchBlankNode(satisfiesArray[0])
	}
}

func matchBundle(doc []interface{}) (target string, extent int, domain []string, index []ld.Node, isBundle bool) {
	var isTempBundle bool
	values := map[string]ld.Node{}
	for _, node := range doc {
		if !isTempBundle {
			target, extent, domain, isTempBundle = matchBundleNode(node)
			if isTempBundle {
				continue
			}
		}

		if node, is := node.(map[string]interface{}); !is || len(node) != 2 {
			return
		} else if id, is := node["@id"].(string); !is || id[:2] != "_:" {
			return
		} else if valueArray, is := node[valueIri].([]interface{}); !is || len(valueArray) != 1 {
			return
		} else if value, is := valueArray[0].(map[string]interface{}); !is {
			return
		} else if iri, is := value["@id"].(string); is && len(value) == 1 {
			if iri[:2] == "_:" {
				values[id] = ld.NewBlankNode(iri)
			} else {
				values[id] = ld.NewIRI(iri)
			}
		} else if literal, is := value["@value"].(string); is {
			if language, is := value["@language"].(string); is && len(value) == 2 {
				values[id] = ld.NewLiteral(literal, ld.RDFLangString, language)
			} else if datatype, is := value["@type"].(string); is && len(value) == 2 {
				values[id] = ld.NewLiteral(literal, datatype, "")
			} else if len(value) == 1 {
				values[id] = ld.NewLiteral(literal, ld.XSDString, "")
			} else {
				return
			}
		}
	}

	if isTempBundle && len(values) <= len(domain) {
		index = make([]ld.Node, len(domain))
		for i, label := range domain {
			if value, has := values[label]; has {
				delete(values, label)
				index[i] = value
			}
		}

		isBundle = len(values) == 0
	}

	return
}

// (I'm really really dumb)
func matchBundleNode(node interface{}) (target string, extent int, domain []string, isBundle bool) {
	if node, is := node.(map[string]interface{}); !is {
		return
	} else if len(node) != 5 {
		return
	} else if types, is := node["@type"].([]interface{}); !is || len(types) != 1 {
		return
	} else if types[0] != bundleIri {
		return
	} else if enumeratesArray, is := node[enumeratesIri].([]interface{}); !is || len(enumeratesArray) != 1 {
		return
	} else if id, is := matchBlankNode(enumeratesArray[0]); !is {
		return
	} else if extentArray, is := node[extentIri].([]interface{}); !is || len(extentArray) != 1 {
		return
	} else if extentNode, is := extentArray[0].(map[string]interface{}); !is || len(extentNode) != 2 {
		return
	} else if extentType, is := extentNode["@type"].(string); !is || extentType != ld.XSDInteger {
		return
	} else if extentValue, is := extentNode["@value"].(string); !is {
		return
	} else if value, err := strconv.Atoi(extentValue); err != nil || value < 1 {
		return
	} else if indexArray, is := node[indexIri].([]interface{}); !is || len(indexArray) != 1 {
		return
	} else if indexNode, is := indexArray[0].(map[string]interface{}); !is || len(indexNode) != 1 {
		return
	} else if indexList, is := indexNode["@list"].([]interface{}); !is {
		return
	} else {
		domain = make([]string, len(indexList))
		for i, node := range indexList {
			if id, is := matchBlankNode(node); is {
				domain[i] = id
			} else {
				return
			}
		}
		return id, value, domain, true
	}
}

func matchBlankNode(node interface{}) (string, bool) {
	if node, is := node.(map[string]interface{}); is && len(node) == 1 {
		if id, is := node["@id"].(string); is && id[:2] == "_:" {
			return id, true
		}
	}
	return "", false
}

func handleBundle(
	graph []int,
	extent int,
	domain []string,
	index []ld.Node,
	quads []*ld.Quad,
	t map[string]interface{},
	entities []map[string]interface{},
	db *DB,
) bool {
	if len(graph) == 1 {
		q := quads[graph[0]]
		if b, is := q.Subject.(*ld.BlankNode); is && q.Predicate.Equal(typeIri) && q.Object.Equal(graphIri) {
			enumerator := fmt.Sprintf("q:%s", b.Attribute)
			graphs := make(chan string, extent)
			go func() {
				var node ld.Node = nil
				if index != nil && len(index) == 1 {
					node = index[0]
				}
				if err := db.Ls(node, extent, graphs); err != nil {
					log.Println(err.Error())
				}
			}()

			for x := 0; x < extent; x++ {
				entities[x] = map[string]interface{}{
					"@type":       "Entity",
					"u:satisfies": t,
				}
				if m := <-graphs; m == "" {
					entities[x]["value"] = []interface{}{}
				} else {
					entities[x]["value"] = []interface{}{
						map[string]interface{}{
							"@id":          m,
							"u:instanceOf": enumerator,
						},
					}
				}
			}
			return true
		}
	}

	return false
}
