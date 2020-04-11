package styx

import (
	"fmt"
	"log"
	"os"
	"testing"

	ld "github.com/piprate/json-gold/ld"
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

var tag = NewPrefixTagScheme("http://example.com/")

func open() *Store {
	fmt.Println("removing path", tmpPath)
	err := os.RemoveAll(tmpPath)
	if err != nil {
		log.Fatalln(err)
	}

	opts := &Options{Path: tmpPath, TagScheme: tag}
	styx, err := NewStore(opts)
	if err != nil {
		log.Fatalln(err)
	}
	return styx
}

func TestIngest(t *testing.T) {
	// Remove old db

	styx := open()
	defer styx.Close()

	err := styx.SetJSONLD(d1, document1, false)
	if err != nil {
		t.Error(err)
		return
	}

	styx.Log()
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

	err = styx.Delete(d1)
	err = styx.Delete(d2)
	if err != nil {
		t.Error(err)
		return
	}

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

	list := styx.List(d2)
	defer list.Close()

	for ; list.Valid(); list.Next() {
		log.Println(list.URI())
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
	"birthDate": { }
}`)
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

	b0, b1 := ld.NewBlankNode("_:b0"), ld.NewBlankNode("_:b1")
	quad := ld.NewQuad(b0, ld.NewIRI("http://schema.org/name"), b1, "")
	iterator, err := styx.Query([]*ld.Quad{quad}, []*ld.BlankNode{b0}, nil)
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

	b0, b1 := ld.NewBlankNode("_:b0"), ld.NewBlankNode("_:b1")
	quad := ld.NewQuad(b0, ld.NewIRI("http://schema.org/name"), b1, "")
	index := []ld.Node{
		ld.NewIRI("http://example.com/d1#_:b1"),
		ld.NewLiteral("Johnny Doe", "", ""),
	}
	iterator, err := styx.Query([]*ld.Quad{quad}, []*ld.BlankNode{b0, b1}, index)
	defer iterator.Close()
	if err != nil {
		t.Error(err)
		return
	}

	iterator.Log()
}
