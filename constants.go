package styx

import (
	fmt "fmt"
	"log"

	"github.com/dgraph-io/badger"
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"
)

// DefaultGraph name of the JSON-LD parser
const DefaultGraph = "@default"

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

func printAssignments(slice []string, index map[string]*Assignment) {
	fmt.Println("printing assignments", slice)
	for _, id := range slice {
		a := index[id]
		fmt.Printf("id: %s\n", id)
		fmt.Println(a.String())
	}
}
