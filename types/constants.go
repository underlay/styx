package types

// Our delimiter of choice
// The "tab" is such a cute concept; idk why she's not more popular
// const tab = byte('\t')
// const newline = byte('\n')

// DefaultGraph name of the JSON-LD parser
const DefaultGraph = "@default"

// Algorithm has to be URDNA2015
const Algorithm = "URDNA2015"

// Format has to be application/nquads
const Format = "application/nquads"

// CounterKey to store the id counter
var CounterKey = []byte{202, 254, 186, 190}

// DocumentPrefix keys track the CIDs of the documents in the database
const DocumentPrefix = byte('d')

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

// var keySizes = map[byte]int{
// 	'a': 3, 'b': 3, 'c': 3,
// 	'i': 2, 'j': 2, 'k': 2,
// 	'l': 2, 'm': 2, 'n': 2,
// 	'x': 1, 'y': 1, 'z': 1,
// }

// InitialCounter is the first uint64 value we start counting from.
// Let's set it to 1 just in case we want to ever use 0 for something special.
const InitialCounter uint64 = 1
