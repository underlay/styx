package db

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"

	cid "github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
	multibase "github.com/multiformats/go-multibase"
	ld "github.com/piprate/json-gold/ld"

	types "github.com/underlay/styx/types"
)

func permuteMajor(permutation uint8, s, p, o []byte) ([]byte, []byte, []byte) {
	if permutation == 0 {
		return s, p, o
	} else if permutation == 1 {
		return p, o, s
	} else {
		return o, s, p
	}
}

func permuteMinor(permutation uint8, s, p, o []byte) ([]byte, []byte, []byte) {
	if permutation == 0 {
		return s, o, p
	} else if permutation == 1 {
		return p, s, o
	} else {
		return o, p, s
	}
}

// DocumentStore is an interface for either an HTTP API or Core API instance
type DocumentStore interface {
	Put(io.Reader) (cid.Cid, error)
	Get(cid.Cid) (io.Reader, error)
}

// HTTPDocumentStore is satisfies core.UnixfsAPI with an HTTP IPFS Shell
type HTTPDocumentStore struct {
	shell *ipfs.Shell
}

// NewHTTPDocumentStore creates a new HTTP Document Store
func NewHTTPDocumentStore(shell *ipfs.Shell) *HTTPDocumentStore {
	return &HTTPDocumentStore{shell}
}

// Put a stream to a multihash
func (api *HTTPDocumentStore) Put(reader io.Reader) (cid.Cid, error) {
	if s, err := api.shell.Add(reader, ipfs.RawLeaves(false)); err != nil {
		return cid.Undef, err
	} else if c, err := cid.Decode(s); err != nil {
		return cid.Undef, err
	} else {
		return cid.NewCidV1(c.Prefix().Codec, c.Hash()), nil
	}
}

// Get a stream by multihash
func (api *HTTPDocumentStore) Get(c cid.Cid) (io.Reader, error) {
	s, err := c.StringOfBase(multibase.Base32)
	if err != nil {
		return nil, err
	}
	return api.shell.Cat(s)
}

// Compile-time type check
var _ DocumentStore = (*HTTPDocumentStore)(nil)

// GetDatasetOptions returns JsonLdOptions for parsing a document into a dataset
func GetDatasetOptions(loader ld.DocumentLoader) *ld.JsonLdOptions {
	options := ld.NewJsonLdOptions("")
	options.ProcessingMode = ld.JsonLd_1_1
	options.DocumentLoader = loader
	options.UseNativeTypes = true
	options.CompactArrays = true
	return options
}

// GetStringOptions returns JsonLdOptions for serializing a dataset into a string
func GetStringOptions(loader ld.DocumentLoader) *ld.JsonLdOptions {
	options := ld.NewJsonLdOptions("")
	options.ProcessingMode = ld.JsonLd_1_1
	options.DocumentLoader = loader
	options.CompactArrays = true
	options.Algorithm = types.Algorithm
	options.Format = types.Format
	options.ProduceGeneralizedRdf = true
	return options
}

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
func ParseMessage(input io.Reader) ([]*ld.Quad, map[string][]int, error) {
	scanner := bufio.NewScanner(input)

	quads := []*ld.Quad{}

	// graphs *always* has an entry for the default graph, even if it's empty.
	graphs := map[string][]int{"": []int{}}

	// scan N-Quad input lines
	lineNumber := 0
	for scanner.Scan() {
		line := scanner.Bytes()
		lineNumber++

		// skip empty lines
		if regexEmpty.Match(line) {
			continue
		}

		// parse quad
		if !regexQuad.Match(line) {
			return nil, nil, fmt.Errorf("Error while parsing N-Quads; invalid quad. line: %d", lineNumber)
		}

		match := regexQuad.FindStringSubmatch(string(line))

		// get subject
		var subject ld.Node
		if match[1] != "" {
			subject = ld.NewIRI(unescape(match[1]))
		} else {
			subject = ld.NewBlankNode(unescape(match[2]))
		}

		// get predicate
		// predicate := ld.NewIRI(unescape(match[3]))
		var predicate ld.Node
		if match[3] != "" {
			predicate = ld.NewIRI(unescape(match[3]))
		} else {
			predicate = ld.NewBlankNode(unescape(match[4]))
		}

		// get object
		var object ld.Node
		if match[5] != "" {
			object = ld.NewIRI(unescape(match[5]))
		} else if match[6] != "" {
			object = ld.NewBlankNode(unescape(match[6]))
		} else {
			language := unescape(match[9])
			var datatype string
			if match[8] != "" {
				datatype = unescape(match[8])
			} else if match[9] != "" {
				datatype = ld.RDFLangString
			} else {
				datatype = ld.XSDString
			}
			unescaped := unescape(match[7])
			object = ld.NewLiteral(unescaped, datatype, language)
		}

		name := ""
		if match[10] != "" {
			name = unescape(match[10])
		} else if match[11] != "" {
			name = unescape(match[11])
		}

		if graph, has := graphs[name]; has {
			graphs[name] = append(graph, len(quads))
		} else {
			graphs[name] = []int{len(quads)}
		}

		quads = append(quads, ld.NewQuad(subject, predicate, object, name))
	}

	return quads, graphs, scanner.Err()
}
