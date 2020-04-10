package styx

import (
	"errors"
)

const (
	XSDString     iri = "AA"
	XSDBoolean    iri = "AB"
	XSDInteger    iri = "AC"
	XSDDouble     iri = "AD"
	RDFType       iri = "AE"
	RDFLangString iri = "AF"
)

// Permutation is a permutation of a triple
type Permutation uint8

const (
	SPO Permutation = iota
	POS
	OSP
	SOP
	PSO
	OPS
)

// ErrInvalidInput indicates that a given dataset was invalid
var ErrInvalidInput = errors.New("Invalid dataset")

// ErrTagScheme indicates that a given URI did not validate the database's tag scheme
var ErrTagScheme = errors.New("Error during Set: URI did not validate the tag scheme")

// ErrEndOfSolutions is a generic out-of-reuslts signal
var ErrEndOfSolutions = errors.New("No more solutions")

// ErrInvalidDomain means that provided domain included blank nodes that were not in the query
var ErrInvalidDomain = errors.New("Invalid domain")

// ErrInvalidIndex means that provided index included blank nodes or that it was too long
var ErrInvalidIndex = errors.New("Invalid index")

// Algorithm has to be URDNA2015
const Algorithm = "URDNA2015"

// Format has to be application/n-quads
const Format = "application/n-quads"

// SequenceKey to store the id counter
var SequenceKey = []byte(":")

// DatasetPrefix keys track the Multihashes of the documents in the database
const DatasetPrefix = byte('/')

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
