package styx

// Algorithm has to be URDNA2015
const Algorithm = "URDNA2015"

// Format has to be application/nquads
const Format = "application/nquads"

// InitialCounter is the first uint64 value we start counting from.
// Let's set it to 1 just in case we want to ever use 0 for something special.

// ConstantPermutation is the value we give to all-constant references.
// We don't even use them for now.
const ConstantPermutation uint8 = 255
const PermutationA uint8 = 0
const PermutationB uint8 = 1
const PermutationC uint8 = 2
const PermutationAB uint8 = 3
const PermutationBC uint8 = 4
const PermutationCA uint8 = 5
const PermutationABC uint8 = 9

// InitialCounter is the first value we write to major, minor, & index keys
const InitialCounter uint64 = 1
