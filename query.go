package styx

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger"
	ipfs "github.com/ipfs/go-ipfs-api"
	"github.com/piprate/json-gold/ld"
)

func query(q map[string]interface{}, sh *ipfs.Shell, db *badger.DB, cb func(AssignmentStack) error) error {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.Explicit = true

	// We'll come back to this flag at the very end
	_, queryHasGraph := q["@graph"]

	flattened, err := proc.Flatten(q, nil, options)
	if err != nil {
		return err
	}

	rdf, err := proc.ToRDF(flattened, options)
	if err != nil {
		return err
	}

	dataset := rdf.(*ld.RDFDataset)

	for graph, quads := range dataset.Graphs {
		fmt.Printf("%s:\n", graph)
		for i, quad := range quads {
			fmt.Printf("%2d: %s %s %s\n", i, quad.Subject.GetValue(), quad.Predicate.GetValue(), quad.Object.GetValue())
		}
	}

	as := getAssignmentStack(dataset)
	return db.View(func(txn *badger.Txn) error {
		for _, am := range as.maps {
			pass, err := solveAssignmentMap(am, as, dataset, txn)
			fmt.Println("solving assignment map", pass, err)
			if err != nil {
				return err
			} else if !pass {
				return nil // TODO: make an error
			}
		}
		flattened, match := flattened.([]interface{})
		if !match {
			return nil // TODO: make an error
		}
		for _, node := range flattened {
			node, match := node.(map[string]interface{})
			if match {
				for key, value := range node {
					if key == "@id" {
						if value, match := value.(string); match && value[:2] == "_:" {
							if index, has := as.deps[value]; has {
								iri := as.maps[index][value].Value
								end := len(iri) - 1
								node[key] = string(iri[1:end])
							}
						} else {
							return nil // TODO: make an error
						}
					} else {
						if values, match := value.([]interface{}); match {
							for _, value := range values {
								if value, match := value.(map[string]interface{}); match {
									if id, has := value["@id"]; has {
										if id, match := id.(string); match && id[:2] == "_:" {
											if index, has := as.deps[id]; has {
												ldNode := unmarshalValue(as.maps[index][id].Value)
												if iri, isIRI := ldNode.(*ld.IRI); isIRI {
													value["@id"] = iri.Value
												} else if literal, isLiteral := ldNode.(*ld.Literal); isLiteral {
													delete(value, "@id")
													value["@value"] = literal.Value
													if literal.Language != "" || literal.Datatype == ld.RDFLangString {
														value["@language"] = literal.Language
													} else if literal.Datatype != "" {
														// TODO: in the future, this is getting renamed to @datatype
														if literal.Datatype == ld.XSDString {
															// do nothing
														} else if literal.Datatype == ld.XSDBoolean {
															parsedBool, err := strconv.ParseBool(literal.Value)
															if err == nil {
																value["@value"] = parsedBool
															} else {
																value["@type"] = literal.Datatype
															}
														} else if literal.Datatype == ld.XSDDouble {
															// f := big.NewFloat(0)
															// err = f.UnmarshalText([]byte(literal.Value))
															f, err := strconv.ParseFloat(literal.Value, 64)
															if err != nil {
																return err
															}
															value["@value"] = f
														} else if literal.Datatype == ld.XSDInteger {
															// z := big.NewInt(0)
															// err = z.UnmarshalText([]byte(literal.Value))
															z, err := strconv.ParseInt(literal.Value, 10, 0)
															if err != nil {
																return err
															}
															value["@value"] = z
														} else {
															value["@type"] = literal.Datatype
														}
													}
												} else {
													fmt.Println("COULD NOT MATCH IRI OR LITERAL", ldNode)
													return nil // TODO: make an error
												}
											}
										}
									}
								} else {
									// Here it's fine if it doesn't match. There will be constant
									// values like {"prop": 100} that are used for constraints.
								}
							}
						} else {
							return nil // TODO: make an error
						}
					}
				}
			} else {
				fmt.Println("no match :-(") // TODO: make an error
			}
		}
		framed, err := proc.Frame(flattened, q, options)
		if err != nil {
			return err
		}

		// Extract a single top-level graph element if necessary
		if graph, hasGraph := framed["@graph"]; !queryHasGraph && hasGraph {
			if array, isArray := graph.([]interface{}); isArray && len(array) == 1 {
				if object, isObject := array[0].(map[string]interface{}); isObject {
					delete(framed, "@graph")
					for key, val := range object {
						framed[key] = val
					}
				}
			}
		}

		b, err := json.MarshalIndent(framed, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(b))

		return cb(as)
	})
}
