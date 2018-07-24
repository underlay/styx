package styx

import (
	"errors"
	"log"

	ld "github.com/piprate/json-gold/ld"
)

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

// Branch is the intermediate value
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
	return len(value) > len(Variable) && value[:len(Variable)] == Variable
}

func scoreTriple(triple Triple, pivot string, frame Frame) Score {
	score := Score{}
	for i, element := range triple {
		if element == pivot {
			score.pivot = append(score.pivot, i)
		} else if isBlankNode(element) || isVariable(element) {
			_, has := frame[element]
			if has {
				score.constant = append(score.constant, i)
			} else {
				score.variable = append(score.variable, i)
			}
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
	j := 0
	for i, triple := range triples {
		score := scoreTriple(triple, pivot, frame)
		scores[i] = score
		// May as well check for singletons on the first pass
		if len(score.pivot) == 1 && len(score.constant) == 2 {
			keys := map[string]bool{}
			p := focus(score.pivot[0])
			a, b, c := index(p)
			var A, B string
			val, has := frame[triple[a]]
			if has {
				A = val.Value
			} else {
				A = triple[a]
			}
			val, has = frame[triple[b]]
			if has {
				B = val.Value
			} else {
				B = triple[b]
			}
			quads := store.minorIndex(p, A, B)
			for _, quad := range quads {
				value := quad.Triple[c]
				array, has := result[value]
				if has {
					result[value] = append(array, quad)
					keys[value] = true
				} else if j == 0 {
					result[value] = []Quad{quad}
				}
			}
			// Remove values that weren't intersected
			if j > 0 {
				deleteIntersect(result, keys)
			}
			j++
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
		var C string
		val, has := frame[triple[c]]
		if has {
			C = val.Value
		} else {
			C = triple[c]
		}
		for value := range result {
			quads := store.minorIndex(p, value, value)
			for _, quad := range quads {
				if quad.Triple[c] == C {
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
	if branch.parent == nil {
		return Branch{}, errors.New("Search failed")
	}
	stack = append([]string{branch.pivot}, stack...)
	branch = *branch.parent
	for branch.index+1 >= len(branch.values) {
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

func getStack(variables map[string][]Triple, frame Frame) ([]string, error) {
	primaries := Frame{}
	for variable, triples := range variables {
		for _, triple := range triples {
			score := scoreTriple(triple, "", frame)
			if len(score.constant) == 2 && len(score.variable) == 1 {
				primaries[variable] = Value{}
				delete(variables, variable)
			}
		}
	}
	stack := make([]string, len(primaries))
	i := 0
	for variable, value := range primaries {
		stack[i] = variable
		frame[variable] = value
		i++
	}
	if len(variables) == 0 {
		return stack, nil
	}
	if len(primaries) > 0 {
		quotient, err := getStack(variables, frame)
		return append(stack, quotient...), err
	}

	return []string{}, errors.New("Could not serialize search path")
}

// ResolveQuery takes the parsed interface{} of any JSON-LD query document
func (store Store) ResolveQuery(doc interface{}) (Branch, error) {
	processor := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	api := ld.NewJsonLdApi()
	expanded, _ := processor.Expand(doc, options)
	dataset, _ := api.ToRDF(expanded, options)
	triples := []Triple{}
	variables := map[string][]Triple{}
	for _, quads := range dataset.Graphs {
		for _, quad := range quads {
			triple := parseQuad(quad)
			for j := 0; j < 3; j++ {
				variable := triple[j]
				if isBlankNode(variable) || isVariable(variable) {
					a, has := variables[variable]
					if has {
						variables[variable] = append(a, triple)
					} else {
						variables[variable] = []Triple{triple}
					}
				}
			}
			triples = append(triples, triple)
		}
	}

	stack, err := getStack(variables, Frame{})
	if err != nil {
		log.Fatalln(err)
	}

	branch := Branch{
		parent: nil,
		values: []Value{},
		index:  0,
		frame:  Frame{},
		pivot:  "",
	}
	return store.search(triples, stack, branch)
}

func (store Store) ResolvePath(root map[string]interface{}, path []string) (Branch, error) {
	pointer := root
	for _, element := range path {
		value, has := pointer[element]
		newPointer := map[string]interface{}{}
		if has {
			switch value := value.(type) {
			case map[string]interface{}:
				newPointer = value
			case []interface{}:
				pointer[element] = append(value, newPointer)
			default:
				pointer[element] = []interface{}{value, newPointer}
			}
		} else {
			pointer[element] = newPointer
		}
		pointer = newPointer
	}
	pointer["@id"] = Variable + "result"
	return store.ResolveQuery(root)
}
