package db

import (
	"regexp"
	"strings"
)

var initialCount uint64 = 1

/*
The following adapted from
https://github.com/piprate/json-gold/blob/cbe4d8e72e79cba3914210e97819a9d7df25a347/ld/serialize_nquads.go
*/

func unescape(str string) string {
	str = strings.Replace(str, "\\\\", "\\", -1)
	str = strings.Replace(str, "\\\"", "\"", -1)
	str = strings.Replace(str, "\\n", "\n", -1)
	str = strings.Replace(str, "\\r", "\r", -1)
	str = strings.Replace(str, "\\t", "\t", -1)
	return str
}

const (
	wso = "[ \\t]*"
	iri = "(?:<([^:]+:[^>]*)>)"

	// https://www.w3.org/TR/turtle/#grammar-production-BLANK_NODE_LABEL

	pnCharsBase = "A-Z" + "a-z" +
		"\u00C0-\u00D6" +
		"\u00D8-\u00F6" +
		"\u00F8-\u02FF" +
		"\u0370-\u037D" +
		"\u037F-\u1FFF" +
		"\u200C-\u200D" +
		"\u2070-\u218F" +
		"\u2C00-\u2FEF" +
		"\u3001-\uD7FF" +
		"\uF900-\uFDCF" +
		"\uFDF0-\uFFFD"
		// TODO:
		//"\u10000-\uEFFFF"

	pnCharsU = pnCharsBase + "_"

	pnChars = pnCharsU +
		"0-9" +
		"-" +
		"\u00B7" +
		"\u0300-\u036F" +
		"\u203F-\u2040"

	blankNodeLabel = "(_:" +
		"(?:[" + pnCharsU + "0-9])" +
		"(?:(?:[" + pnChars + ".])*(?:[" + pnChars + "]))?" +
		")"

	bnode = blankNodeLabel

	plain    = "\"([^\"\\\\]*(?:\\\\.[^\"\\\\]*)*)\""
	datatype = "(?:\\^\\^" + iri + ")"
	language = "(?:@([a-z]+(?:-[a-zA-Z0-9]+)*))"
	literal  = "(?:" + plain + "(?:" + datatype + "|" + language + ")?)"
	ws       = "[ \\t]+"

	subject  = "(?:" + iri + "|" + bnode + ")" + ws
	property = "(?:" + iri + "|" + bnode + ")" + ws // iri + ws
	object   = "(?:" + iri + "|" + bnode + "|" + literal + ")" + wso
	graph    = "(?:\\.|(?:(?:" + iri + "|" + bnode + ")" + wso + "\\.))"
)

var regexWSO = regexp.MustCompile(wso)

var regexEOLN = regexp.MustCompile("(?:\\r\\n)|(?:\\n)|(?:\\r)")

var regexEmpty = regexp.MustCompile("^" + wso + "$")

// define quad part regexes

var regexSubject = regexp.MustCompile("(?:" + iri + "|" + bnode + ")" + ws)
var regexProperty = regexp.MustCompile("(?:" + iri + "|" + bnode + ")" + ws) // regexp.MustCompile(iri + ws)
var regexObject = regexp.MustCompile("(?:" + iri + "|" + bnode + "|" + literal + ")" + wso)
var regexGraph = regexp.MustCompile("(?:\\.|(?:(?:" + iri + "|" + bnode + ")" + wso + "\\.))")

// full quad regex

var regexQuad = regexp.MustCompile("^" + wso + subject + property + object + graph + wso + "$")

// ParseMessage parses RDF in the form of N-Quads from io.Reader, []byte or string.
// func ParseMessage(input io.Reader) ([]*ld.Quad, map[string][]int, error) {
// 	scanner := bufio.NewScanner(input)

// 	quads := []*ld.Quad{}

// 	// graphs *always* has an entry for the default graph, even if it's empty.
// 	graphs := map[string][]int{"": []int{}}

// 	// scan N-Quad input lines
// 	lineNumber := 0
// 	for scanner.Scan() {
// 		line := scanner.Bytes()
// 		lineNumber++

// 		// skip empty lines
// 		if regexEmpty.Match(line) {
// 			continue
// 		}

// 		// parse quad
// 		if !regexQuad.Match(line) {
// 			return nil, nil, fmt.Errorf("Error while parsing N-Quads; invalid quad. line: %d", lineNumber)
// 		}

// 		match := regexQuad.FindStringSubmatch(string(line))

// 		// get subject
// 		var subject ld.Node
// 		if match[1] != "" {
// 			subject = ld.NewIRI(unescape(match[1]))
// 		} else {
// 			subject = ld.NewBlankNode(unescape(match[2]))
// 		}

// 		// get predicate
// 		// predicate := ld.NewIRI(unescape(match[3]))
// 		var predicate ld.Node
// 		if match[3] != "" {
// 			predicate = ld.NewIRI(unescape(match[3]))
// 		} else {
// 			predicate = ld.NewBlankNode(unescape(match[4]))
// 		}

// 		// get object
// 		var object ld.Node
// 		if match[5] != "" {
// 			object = ld.NewIRI(unescape(match[5]))
// 		} else if match[6] != "" {
// 			object = ld.NewBlankNode(unescape(match[6]))
// 		} else {
// 			language := unescape(match[9])
// 			var datatype string
// 			if match[8] != "" {
// 				datatype = unescape(match[8])
// 			} else if match[9] != "" {
// 				datatype = ld.RDFLangString
// 			} else {
// 				datatype = ld.XSDString
// 			}
// 			unescaped := unescape(match[7])
// 			object = ld.NewLiteral(unescaped, datatype, language)
// 		}

// 		name := ""
// 		if match[10] != "" {
// 			name = unescape(match[10])
// 		} else if match[11] != "" {
// 			name = unescape(match[11])
// 		}

// 		if graph, has := graphs[name]; has {
// 			graphs[name] = append(graph, len(quads))
// 		} else {
// 			graphs[name] = []int{len(quads)}
// 		}

// 		quads = append(quads, ld.NewQuad(subject, predicate, object, name))
// 	}

// 	return quads, graphs, scanner.Err()
// }
