package styx

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"
	rdf "github.com/underlay/go-rdfjs"
)

var d1 = "http://example.com/d1"
var d2 = "http://example.com/d2"

var document1 = `{
	"@context": {
		"@vocab": "http://schema.org/",
		"xsd": "http://www.w3.org/2001/XMLSchema#",
		"prov": "http://www.w3.org/ns/prov#",
		"prov:generatedAtTime": { "@type": "xsd:dateTime" },
		"birthDate": { "@type": "xsd:date" }
	},
	"prov:generatedAtTime": "2019-07-24T16:46:05.751Z",
	"@graph": {
		"@type": "Person",
		"name": ["John Doe", "Johnny Doe"],
		"birthDate": "1996-02-02",
		"knows": {
			"@id": "http://people.com/jane",
			"@type": "Person",
			"name": "Jane Doe",
			"familyName": { "@value": "Doe", "@language": "en" },
			"birthDate": "1995-01-01"
		}
	}
}`

var document2 = `{
	"@context": {
		"@vocab": "http://schema.org/",
		"xsd": "http://www.w3.org/2001/XMLSchema#",
		"birthDate": { "@type": "xsd:date" }
	},
	"@type": "Person",
	"name": "Johnanthan Appleseed",
	"birthDate": "1780-01-10",
	"knows": { "@id": "http://people.com/jane" }
}`

func open() *Store {
	fmt.Println("removing path", tmpPath)
	err := os.RemoveAll(tmpPath)
	if err != nil {
		log.Fatalln(err)
	}

	tags := NewPrefixTagScheme("http://example.com/")
	config := &Config{TagScheme: tags, Dictionary: IriDictionary}
	// config := &Config{Path: tmpPath, TagScheme: tags, Dictionary: StringDictionary}
	styx, err := NewStore(config, badger.DefaultOptions(tmpPath))
	if err != nil {
		log.Fatalln(err)
	}
	return styx
}

func TestSet(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(err)
		return
	}

	styx.Log()
}

func TestGet(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(err)
		return
	}

	quads, err := styx.Get(rdf.NewNamedNode(d1))
	for _, quad := range quads {
		log.Println(quad.String())
	}
}

func TestDelete(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(err)
		return
	}

	styx.Log()
	err = styx.SetJSONLD(d2, document2, false)
	if err != nil {
		t.Error(err)
		return
	}

	log.Println("Deleting")

	err = styx.Delete(rdf.NewNamedNode(d1))
	err = styx.Delete(rdf.NewNamedNode(d2))
	if err != nil {
		t.Error(err)
		return
	}

	log.Println("Logging again")
	styx.Log()
}

func TestList(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(err)
		return
	}

	err = styx.SetJSONLD(d2, document2, false)
	if err != nil {
		t.Error(err)
		return
	}

	styx.Log()

	list := styx.List(nil)
	defer list.Close()

	for node := list.Next(); node != nil; node = list.Next() {
		log.Println(node.String())
	}
}

func TestSPO(t *testing.T) {
	styx := open()
	defer styx.Close()
	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(t)
		return
	}

	iterator, err := styx.QueryJSONLD(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@id": "http://people.com/jane",
	"name": { }
}`)
	defer iterator.Close()
	if err != nil {
		t.Error(err)
		return
	}

	iterator.Log()
}

func TestOPS(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(t)
		return
	}

	iterator, err := styx.QueryJSONLD(`{
	"@context": { "@vocab": "http://schema.org/" },
	"name": "Jane Doe"
}`)
	defer iterator.Close()
	if err != nil {
		t.Error(err)
		return
	}

	iterator.Log()
}

func TestSimpleQuery(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(t)
		return
	}

	iterator, err := styx.QueryJSONLD(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "Person",
	"birthDate": { "@id": "?:foo" },
	"name": { "@id": "?:bar" }
}`)
	// 	iterator, err := styx.QueryJSONLD(`{
	// 	"@context": { "@vocab": "http://schema.org/" },
	// 	"@type": "Person",
	// 	"birthDate": { "@id": "?:foo" },
	// 	"name": {}
	// }`)
	defer iterator.Close()
	if err != nil {
		t.Error(err)
		return
	}

	iterator.Log()
}

func TestSimpleQuery2(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(t)
		return
	}

	iterator, err := styx.QueryJSONLD(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "Person",
	"knows": {
		"name": "Jane Doe"
	}
}`)
	defer iterator.Close()
	if err != nil {
		t.Error(err)
		return
	}

	iterator.Log()
}

func TestDomainQuery(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(t)
		return
	}

	err = styx.SetJSONLD(d2, document2, false)
	if err != nil {
		t.Error(t)
		return
	}

	v0, b0 := rdf.NewVariable("v0"), rdf.NewBlankNode("b0")
	quad := rdf.NewQuad(v0, rdf.NewNamedNode("http://schema.org/name"), b0, nil)
	iterator, err := styx.Query([]*rdf.Quad{quad}, []rdf.Term{v0}, nil)
	defer iterator.Close()
	if err != nil {
		t.Error(err)
		return
	}

	iterator.Log()
}

func TestIndexQuery(t *testing.T) {
	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(t)
		return
	}

	err = styx.SetJSONLD(d2, document2, false)
	if err != nil {
		t.Error(t)
		return
	}

	v0, b0 := rdf.NewVariable("v0"), rdf.NewBlankNode("b0")
	quad := rdf.NewQuad(v0, rdf.NewNamedNode("http://schema.org/name"), b0, nil)
	index := []rdf.Term{
		// rdf.NewNamedNode("http://example.com/d2#b0"),
		rdf.NewNamedNode("http://example.com/d1#b1"),
		rdf.NewLiteral("Johnny Doe", "", nil),
	}
	iterator, err := styx.Query([]*rdf.Quad{quad}, []rdf.Term{v0, b0}, index)
	defer iterator.Close()
	if err != nil {
		t.Error(err)
		return
	}

	iterator.Log()
}
