package styx

import (
	"errors"
	"fmt"

	ld "github.com/piprate/json-gold/ld"
)

const variable = "http://underlay.mit.edu/query#"

// The Frame is a map of stuff
type Frame map[string]Value

// A Value is a value tagged with quads that source that value
type Value struct {
	Value   string
	Sources []Quad
}

// Score is a characterization of Triple relative to a frame
type Score struct {
	pivot    []int
	variable []int
	constant []int
}

type Branch struct {
	parent *Branch
	values []Value
	pivot  string
	index  int
	frame  Frame
}

func parseQuad(quad *ld.Quad) Triple {
	subject := quad.Subject.GetValue()
	predicate := quad.Predicate.GetValue()
	object := quad.Object.GetValue()
	return Triple{subject, predicate, object}
}

func isBlankNode(value string) bool {
	return len(value) > 2 && value[:2] == "_:"
}

func isVariable(value string) bool {
	return len(value) > len(variable) && value[:len(variable)] == variable
}

func scoreTriple(triple Triple, pivot string, frame Frame) Score {
	score := Score{}
	for i, element := range triple {
		if element == pivot {
			score.pivot = append(score.pivot, i)
		} else if isBlankNode(element) || isVariable(element) {
			score.variable = append(score.variable, i)
		} else {
			score.constant = append(score.constant, i)
		}
	}
	return score
}

func deleteIntersect(result map[string][]Quad, keys map[string]bool) {
	for key := range result {
		val, has := keys[key]
		if !(has && val) {
			delete(result, key)
		}
	}
}

func (store Store) dissolve(triples []Triple, pivot string, frame Frame) map[string][]Quad {
	scores := make([]Score, len(triples))
	result := map[string][]Quad{}
	diads := []int{}
	triads := false
	for i, triple := range triples {
		score := scoreTriple(triple, pivot, frame)
		scores[i] = score
		// May as well check for singletons on the first pass
		if len(score.pivot) == 1 && len(score.constant) == 2 {
			keys := map[string]bool{}
			p := focus(score.pivot[0])
			a, b, c := index(p)
			quads := store.minorIndex(p, triple[a], triple[b])
			for _, quad := range quads {
				value := quad.Triple[c]
				array, has := result[value]
				if has {
					result[value] = append(array, quad)
					keys[value] = true
				} else if i == 0 {
					result[value] = []Quad{quad}
				}
			}
			// Remove values that weren't intersected
			if i > 0 {
				deleteIntersect(result, keys)
			}
		} else if len(score.pivot) == 2 && len(score.constant) == 1 {
			diads = append(diads, i)
		} else if len(score.pivot) == 3 {
			triads = true
		}
	}
	for _, i := range diads {
		triple := triples[i]
		score := scores[i]
		p := focus(score.constant[0])
		_, _, c := index(p) // triple[a] == triple[b] == pivot
		keys := map[string]bool{}
		for value := range result {
			quads := store.minorIndex(p, value, value)
			for _, quad := range quads {
				if quad.Triple[c] == triple[c] {
					result[value] = append(result[value], quad)
					keys[value] = true
				}
			}
		}
		// Remove values that weren't intersected
		deleteIntersect(result, keys)
	}
	if triads {
		keys := map[string]bool{}
		for value := range result {
			quads := store.minorIndex(0, value, value)
			for _, quad := range quads {
				if quad.Triple[2] == value {
					result[value] = append(result[value], quad)
					keys[value] = true
				}
			}
		}
		// Remove values that weren't intersected
		deleteIntersect(result, keys)
	}
	return result
}

func (store Store) search(triples []Triple, stack []string, branch Branch) (Branch, error) {
	if len(stack) == 0 {
		return branch, nil
	}
	pivot := stack[0]
	result := store.dissolve(triples, pivot, branch.frame)
	if len(result) > 0 {
		// result = sortResult(result)
		values := make([]Value, len(result))
		i := 0
		for value, sources := range result {
			values[i] = Value{value, sources}
			i++
		}

		frame := Frame{pivot: values[0]}
		// Copy previous bindings
		for key, val := range branch.frame {
			frame[key] = val
		}
		next := Branch{
			parent: &branch,
			values: values,
			index:  0,
			frame:  frame,
			pivot:  pivot,
		}
		return store.search(triples, stack[1:], next)
	}
	// backtrack until you find a resumable branch
	stack = append([]string{branch.pivot}, stack...)
	branch = *branch.parent
	if branch.parent == nil {
		return Branch{}, errors.New("Search failed")
	}
	for branch.index+1 == len(branch.values) {
		if branch.parent == nil {
			return Branch{}, errors.New("Search failed")
		}
		stack = append([]string{branch.pivot}, stack...)
		branch = *branch.parent
	}
	branch.index++
	branch.frame[branch.pivot] = branch.values[branch.index]
	return store.search(triples, stack, branch)
}

// ResolveQuery takes the parsed interface{} of any JSON-LD query document
func (store Store) ResolveQuery(doc interface{}) {
	processor := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	api := ld.NewJsonLdApi()
	expanded, _ := processor.Expand(doc, options)
	dataset, _ := api.ToRDF(expanded, options)
	triples := []Triple{}
	variables := map[string]int{}
	for _, quads := range dataset.Graphs {
		for _, quad := range quads {
			triple := parseQuad(quad)
			score := scoreTriple(triple, "", Frame{})
			if len(score.constant) == 2 && len(score.variable) == 1 {
				variable := triple[score.variable[0]]
				count, has := variables[variable]
				if has {
					variables[variable] = count + 1
				} else {
					variables[variable] = 1
				}
			} else if len(score.variable) > 0 {
				for _, i := range score.variable {
					variable := triple[i]
					_, has := variables[variable]
					if !has {
						variables[variable] = 0
					}
				}
			}
			triples = append(triples, triple)
		}
	}
	stack := []string{"foo"}
	pivot := stack[0]
	stack = stack[1:]
	branch := Branch{
		parent: nil,
		values: []Value{},
		index:  0,
		frame:  Frame{},
		pivot:  pivot,
	}
	branch, err := store.search(triples, stack, branch)
	fmt.Println(branch, err)
}
