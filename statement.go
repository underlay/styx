package styx

import (
	"fmt"
	"strconv"
	"strings"

	rdf "github.com/underlay/go-rdfjs"
)

// A Statement is a reference to a specific quad in a specific dataset
type Statement struct {
	base  iri
	index uint64
	graph ID
}

func (statement *Statement) String() string {
	i := strconv.FormatUint(statement.index, 32)
	return fmt.Sprintf("%s\t%s\t%s\n", statement.base, i, statement.graph)
}

// URI returns the URI for the statement using path syntax
func (statement *Statement) URI(dictionary Dictionary) string {
	base, _ := dictionary.GetTerm(ID(statement.base), rdf.Default)
	return fmt.Sprintf("%s/%d", base, statement.index)
}

// Graph returns the URI for the statement's graph
func (statement *Statement) Graph(dictionary Dictionary) rdf.Term {
	graph, _ := dictionary.GetTerm(statement.graph, rdf.Default)
	switch graph := graph.(type) {
	case *rdf.NamedNode:
		return graph
	default:
		base, _ := dictionary.GetTerm(ID(statement.base), rdf.Default)
		return rdf.NewNamedNode(base.Value() + "#" + graph.Value())
	}
}

func getStatements(val []byte) ([]*Statement, error) {
	lines := strings.Split(string(val), "\n")
	if len(lines) < 2 {
		return nil, nil
	}

	statements := make([]*Statement, len(lines)-1)
	for i, line := range lines[:len(lines)-1] {
		terms := strings.Split(line, "\t")
		if len(terms) == 3 {
			index, err := strconv.ParseUint(string(terms[1]), 32, 64)
			if err != nil {
				return nil, err
			}
			statements[i] = &Statement{
				base:  iri(terms[0]),
				index: index,
				graph: ID(terms[2]),
			}
		}
	}

	// matches := statementPattern.FindAllSubmatch(val, -1)
	// for i, match := range matches {
	// 	index, err := strconv.ParseUint(string(match[2]), 32, 64)
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	statements[i] = &Statement{
	// 		Base:  iri(match[1]),
	// 		Index: index,
	// 		Graph: ID(match[3]),
	// 	}
	// }
	return statements, nil
}
