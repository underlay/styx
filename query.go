package styx

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger"
	ipfs "github.com/ipfs/go-ipfs-api"
	"github.com/piprate/json-gold/ld"
)

func query(q interface{}, sh *ipfs.Shell, db *badger.DB, cb func(AssignmentStack) error) error {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.Explicit = true
	var needToExtractGraph bool
	if object, isObject := q.(map[string]interface{}); isObject {
		if _, hasGraph := object["@graph"]; !hasGraph {
			needToExtractGraph = true
		}
	}

	flattened, err := proc.Flatten(q, nil, options)
	fmt.Println("flattened!", flattened)
	if err != nil {
		return err
	}

	rdf, err := proc.ToRDF(flattened, options)
	if err != nil {
		return err
	}

	dataset := rdf.(*ld.RDFDataset)
	as := getAssignmentStack(dataset)
	fmt.Println("about to query or something")
	return db.View(func(txn *badger.Txn) error {
		for _, am := range as.maps {
			pass, err := solveAssignmentMap(am, as, dataset, txn)
			if err != nil {
				return err
			} else if !pass {
				return nil // TODO: make an error
			}
		}
		flattened, match := flattened.([]interface{})
		if !match {
			fmt.Println("the thing does not match!!!!!", flattened)
			return nil // TODO: make an error
		}
		for _, node := range flattened {
			node, match := node.(map[string]interface{})
			if match {
				fmt.Println("match!", node)
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
		if graph, hasGraph := framed["@graph"]; needToExtractGraph && hasGraph {
			if array, isArray := graph.([]interface{}); isArray && len(array) == 1 {
				if object, isObject := array[0].(map[string]interface{}); isObject {
					delete(framed, "@graph")
					for key, val := range object {
						framed[key] = val
					}
				}
			}
		}

		fmt.Println("hahaha the thing got frammmeeed")
		b, err := json.Marshal(framed)
		if err != nil {
			return err
		}
		fmt.Println(string(b))
		return cb(as)
	})
}
