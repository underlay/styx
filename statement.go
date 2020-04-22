package styx

import (
	"fmt"
	"strconv"
	"strings"

	rdf "github.com/underlay/go-rdfjs"
)

// A Statement is a reference to a specific quad in a specific dataset
type Statement struct {
	Base  iri
	Index uint64
	Graph ID
}

func (statement *Statement) String() string {
	i := strconv.FormatUint(statement.Index, 32)
	return fmt.Sprintf("%s\t%s\t%s\n", statement.Base, i, statement.Graph)
}

// URI returns the URI for the statement using path syntax
func (statement *Statement) URI(dictionary Dictionary) string {
	base, _ := dictionary.GetTerm(ID(statement.Base), rdf.Default)
	return fmt.Sprintf("%s/%d", base, statement.Index)
}

// var statementPattern = regexp.MustCompile("([a-zA-Z0-9+/]*)\t([a-z0-9]+)\t([a-zA-Z0-9+/]*(?:#[a-zA-Z0-9-_]*)?)")

func getStatements(val []byte, dictionary Dictionary) ([]*Statement, error) {
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
				Base:  iri(terms[0]),
				Index: index,
				Graph: ID(terms[2]),
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
