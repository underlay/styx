package types

import ld "github.com/piprate/json-gold/ld"

// Permutation is a permutation of a triple
type Permutation uint8

// C is the constant (no blank nodes) Permutation
const C Permutation = 255 // zoot zoot
// SPO is the blank (all blank nodes) Permutation
const SPO Permutation = 9 // pSPO % 3 == 0
const (
	// S is the Permutation where the Subject is the only blank node
	S Permutation = iota
	// P is the Permutation where the Predicate is the only blank node
	P
	// O is the Permutation where the Object is the only blank node
	O
	// SP is the Permutation where the Subject and Predicate are both blank nodes
	SP // it's important that pSP % 3 == pS, pPO % 3 == pP, etc
	// PO is the Permutation where the Predicate and Object are both blank nodes
	PO
	// OS is the Permutation where the Object and Subject are both blank nodes
	OS
)

// Algorithm has to be URDNA2015
const Algorithm = "URDNA2015"

// Format has to be application/n-quads
const Format = "application/n-quads"

// SequenceKey to store the id counter
var SequenceKey = []byte(">")

// DatasetPrefix keys track the Multihashes of the documents in the database
const DatasetPrefix = byte('<')

// ValuePrefix keys translate uint64 ids to ld.Node values
const ValuePrefix = byte('p')

// IndexPrefix keys translate ld.Node values to uint64 ids
const IndexPrefix = byte('q')

// TriplePrefixes address the value indices
var TriplePrefixes = [3]byte{'a', 'b', 'c'}

// TriplePrefixMap inverts TriplePrefixes
var TriplePrefixMap = map[byte]uint8{'a': 0, 'b': 1, 'c': 2}

// MajorPrefixes address the "counter-clockwise" indices {spo, pos, osp}
var MajorPrefixes = [3]byte{'i', 'j', 'k'}

// MajorPrefixMap inverts MajorPrefixes
var MajorPrefixMap = map[byte]uint8{'i': 0, 'j': 1, 'k': 2}

// MinorPrefixes address the "clockwise" indices {sop, pso, ops}
var MinorPrefixes = [3]byte{'x', 'y', 'z'}

// MinorPrefixMap inverts MinorPrefixes
var MinorPrefixMap = map[byte]uint8{'x': 0, 'y': 1, 'z': 2}

// MajorMatrix indexes the major permutations
var MajorMatrix = [3][3]uint8{
	[3]uint8{0, 1, 2},
	[3]uint8{1, 2, 0},
	[3]uint8{2, 0, 1},
}

// MinorMatrix indexes the minor permutations
var MinorMatrix = [3][3]uint8{
	[3]uint8{0, 2, 1},
	[3]uint8{1, 0, 2},
	[3]uint8{2, 1, 0},
}

// Permute indexes the given ids by the given permutation
func Permute(permutation uint8, matrix [3][3]uint8, ids [3][]byte) ([]byte, []byte, []byte) {
	row := matrix[permutation]
	return ids[row[0]], ids[row[1]], ids[row[2]]
}

// GetNode just indexes the Permutation into the appropriate term of the quad
func GetNode(quad *ld.Quad, place Permutation) (node ld.Node) {
	switch place {
	case 0:
		node = quad.Subject
	case 1:
		node = quad.Predicate
	case 2:
		node = quad.Object
	case 3:
		node = quad.Graph
	}
	return
}
