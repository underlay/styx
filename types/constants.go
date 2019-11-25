package types

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
