package styx

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/dgraph-io/badger"
	ipfs "github.com/ipfs/go-ipfs-api"
	"github.com/piprate/json-gold/ld"
)

func query(q map[string]interface{}, sh *ipfs.Shell, db *badger.DB, cb func(result, prov, fail map[string]interface{}) error) error {
	proc := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	options.Explicit = true

	var prov, fail map[string]interface{}

	// We'll come back to this flag at the very end. This replicates the
	// funcitonality of the "omit graph flag" in json-ld-1.1
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

	// Print the RDF dataset
	printDataset(dataset)

	as := getAssignmentStack(dataset)
	return db.View(func(txn *badger.Txn) error {
		for _, am := range as.maps {
			// The first thing we do is populate the assignment map with counter stats
			id, ref, err := countReferences(am, as, dataset, txn)
			if err != nil {
				return err
			} else if ref != nil {
				return fmt.Errorf("reference %s could not be solved: %s", id, ref.String())
			}

			// Once they've been counted and sorted, we solve the entire map in unison
			pass, err := solveAssignmentMap(am, as, dataset, txn)
			if err != nil {
				return err
			} else if !pass {
				return errors.New("assignment map unsatisfied")
			}
		}
		flattenedSlice, match := flattened.([]interface{})
		if !match {
			return errors.New("flattened graph is not a slice")
		}
		for _, node := range flattenedSlice {
			if node, match := node.(map[string]interface{}); match {
				for key, value := range node {
					if key == "@id" {
						if value, match := value.(string); match && value[:2] == "_:" {
							index := as.deps[value]
							iri := as.maps[index][value].Value
							end := len(iri) - 1
							node[key] = string(iri[1:end])
						}
					} else if values, match := value.([]interface{}); match {
						for _, value := range values {
							if value, match := value.(map[string]interface{}); match {
								if id, has := value["@id"]; has {
									if id, match := id.(string); match && id[:2] == "_:" {
										index := as.deps[id]
										node := unmarshalValue(as.maps[index][id].Value)
										if iri, isIRI := node.(*ld.IRI); isIRI {
											value["@id"] = iri.Value
										} else if literal, isLiteral := node.(*ld.Literal); isLiteral {
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
											return fmt.Errorf("could not match iri or literal: %s", node.GetValue())
										}
									}
								}
							}
						}
					} else {
						return fmt.Errorf("unexpected value in the flattened graph: %s %v", key, value)
					}
				}
			} else {
				return fmt.Errorf("unexpected node in the flattened graph: %v", node)
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

		return cb(framed, prov, fail)
	})
}
