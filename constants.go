package main

import (
	fmt "fmt"
	"log"

	badger "github.com/dgraph-io/badger"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"
)

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

// ConstantPermutation is the value we give to all-constant references.
// We don't even use them for now.
const constantPermutation uint8 = 255
const permutationA uint8 = 0
const permutationB uint8 = 1
const permutationC uint8 = 2
const permutationAB uint8 = 3
const permutationBC uint8 = 4
const permutationCA uint8 = 5
const permutationABC uint8 = 9

// CounterKey to store the id counter
var CounterKey = []byte{202, 254, 186, 190}

// ValuePrefix keys translate uint64 ids to ld.Node values
const ValuePrefix = byte('p')

// IndexPrefix keys translate ld.Node values to uint64 ids
const IndexPrefix = byte('q')

// TriplePrefixes address the value indices
var TriplePrefixes = [3]byte{'a', 'b', 'c'}
var triplePrefixMap = map[byte]uint8{'a': 0, 'b': 1, 'c': 2}

// MajorPrefixes address the "counter-clockwise" indices {spo, pos, osp}
var MajorPrefixes = [3]byte{'i', 'j', 'k'}
var majorPrefixMap = map[byte]uint8{'i': 0, 'j': 1, 'k': 2}

// MinorPrefixes address the "clockwise" indices {sop, pso, ops}
var MinorPrefixes = [3]byte{'x', 'y', 'z'}
var minorPrefixMap = map[byte]uint8{'x': 0, 'y': 1, 'z': 2}

var keySizes = map[byte]int{
	'a': 3, 'b': 3, 'c': 3,
	'i': 2, 'j': 2, 'k': 2,
	'l': 2, 'm': 2, 'n': 2,
	'x': 1, 'y': 1, 'z': 1,
}

// InitialCounter is the first uint64 value we start counting from.
// Let's set it to 1 just in case we want to ever use 0 for something special.
const InitialCounter uint64 = 1

var iteratorOptions = badger.IteratorOptions{
	PrefetchValues: false,
}

func (source *Source) toCompactString() string {
	c, err := cid.Parse(source.Cid)
	if err != nil {
		log.Fatalln(err)
	}
	return fmt.Sprintf("%s#%s[%d]", c.String(), source.Graph, source.Index)
}

func sourcesToString(sources []*Source) string {
	s := "["
	for i, source := range sources {
		if i > 0 {
			s += ", "
		}
		s += source.toCompactString()
	}
	return s + "]"
}

func printCodexMap(c *CodexMap) {
	fmt.Println("----- Codex Map -----")
	for _, id := range c.Slice {
		fmt.Printf("---- %s ----\n%s\n", id, c.Index[id].String())
	}
	fmt.Println("----- End of Codex Map -----")
}
func printDataset(dataset *ld.RDFDataset) {
	for graph, quads := range dataset.Graphs {
		fmt.Printf("%s:\n", graph)
		for i, quad := range quads {
			fmt.Printf("%2d: %s %s %s\n", i, quad.Subject.GetValue(), quad.Predicate.GetValue(), quad.Object.GetValue())
		}
	}
}

func printAssignments(assignmentMap *AssignmentMap) {
	fmt.Println("printing assignments", assignmentMap.Slice)
	for _, id := range assignmentMap.Slice {
		a := assignmentMap.Index[id]
		fmt.Printf("id: %s\n", id)
		fmt.Println(a.String())
	}
}
