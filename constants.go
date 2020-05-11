package styx

import (
	"errors"

	ld "github.com/piprate/json-gold/ld"
)

// Permutation is a permutation of a triple
type Permutation uint8

const (
	// SPO is the subject-predicate-object permutation
	SPO Permutation = iota
	// POS is the predicate-object-subject permutation
	POS
	// OSP is the object-subject-predicate permutation
	OSP
	// SOP is the subject-object-predicate permutation
	SOP
	// PSO is the predicate-subject-object permutation
	PSO
	// OPS is the object-predicate-subject permutation
	OPS
)

// Do NOT modify the order of these! Only append to the slice.
var constants = []string{
	ld.XSDString,
	ld.XSDBoolean,
	ld.XSDInteger,
	ld.XSDDouble,
	ld.XSDFloat,
	ld.XSDDecimal,
	ld.RDFType,
	ld.RDFFirst,
	ld.RDFRest,
	ld.RDFNil,
	ld.RDFPlainLiteral,
	ld.RDFXMLLiteral,
	ld.RDFJSONLiteral,
	ld.RDFObject,
	ld.RDFLangString,
	ld.RDFList,
}

// ErrInvalidInput indicates that a given dataset was invalid
var ErrInvalidInput = errors.New("Invalid dataset")

// ErrTagScheme indicates that a given URI did not validate the database's tag scheme
var ErrTagScheme = errors.New("URI did not validate the tag scheme")

// ErrEndOfSolutions is a generic out-of-reuslts signal
var ErrEndOfSolutions = errors.New("No more solutions")

// ErrEmptyInterset indicates that a constraint set had an empty join
var ErrEmptyInterset = errors.New("Empty intersection")

// ErrInvalidDomain means that provided domain included blank nodes that were not in the query
var ErrInvalidDomain = errors.New("Invalid domain")

// ErrInvalidIndex means that provided index included blank nodes or that it was too long
var ErrInvalidIndex = errors.New("Invalid index")

// Algorithm has to be URDNA2015
const Algorithm = "URDNA2015"

// Format has to be application/n-quads
const Format = "application/n-quads"

// SequenceKey to store the id counter
var SequenceKey = []byte("#")

// DatasetPrefix keys store the datasets in the database
const DatasetPrefix = byte(':')

// ValueToIDPrefix keys translate string IRIs to uint64 ids
const ValueToIDPrefix = byte('>')

// IDToValuePrefix keys translate uint64 ids to string IRIs
const IDToValuePrefix = byte('<')

// UnaryPrefix keys translate ld.Node values to uint64 ids
const UnaryPrefix = byte('u')

// TernaryPrefixes address the ternary indices
var TernaryPrefixes = [3]byte{'a', 'b', 'c'}

// BinaryPrefixes address the binary indices
var BinaryPrefixes = [6]byte{'i', 'j', 'k', 'l', 'm', 'n'}
