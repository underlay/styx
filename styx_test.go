package styx

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	ld "github.com/piprate/json-gold/ld"
)

var d1 = "http://example.com/d1"
var d2 = "http://example.com/d2"

var sampleDataBytes = []byte(`{
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
}`)

var sampleDataBytes2 = []byte(`{
	"@context": {
		"@vocab": "http://schema.org/",
		"xsd": "http://www.w3.org/2001/XMLSchema#",
		"birthDate": { "@type": "xsd:date" }
	},
	"@type": "Person",
	"name": "Johnanthan Appleseed",
	"birthDate": "1780-01-10",
	"knows": { "@id": "http://people.com/jane" }
}`)

var sampleData, sampleData2 map[string]interface{}

var tag = NewPrefixTagScheme("http://example.com/")

func init() {
	var err error
	_ = json.Unmarshal(sampleDataBytes, &sampleData)
	_ = json.Unmarshal(sampleDataBytes2, &sampleData2)
	if err != nil {
		log.Fatalln(err)
	}
}

func TestIngest(t *testing.T) {
	// Remove old db
	fmt.Println("removing path", DefaultPath)
	err := os.RemoveAll(DefaultPath)
	if err != nil {
		t.Fatal(err)
	}

	db, err := OpenDB(DefaultPath, tag)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	_, err = IngestJSONLd(db, d1, sampleData, false)
	if err != nil {
		t.Error(err)
		return
	}

	db.Log()
}

func TestDelete(t *testing.T) {
	// Remove old db
	fmt.Println("removing path", DefaultPath)
	err := os.RemoveAll(DefaultPath)
	if err != nil {
		t.Fatal(err)
	}

	db, err := OpenDB(DefaultPath, tag)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	_, err = IngestJSONLd(db, d1, sampleData, false)
	if err != nil {
		t.Error(err)
		return
	}

	db.Log()
	_, err = IngestJSONLd(db, d2, sampleData2, false)
	if err != nil {
		t.Error(err)
		return
	}

	err = db.Delete(d1)
	err = db.Delete(d2)
	if err != nil {
		t.Error(err)
		return
	}

	db.Log()
}

func TestList(t *testing.T) {
	// Remove old db
	fmt.Println("removing path", DefaultPath)
	err := os.RemoveAll(DefaultPath)
	if err != nil {
		t.Fatal(err)
	}

	db, err := OpenDB(DefaultPath, tag)
	if err != nil {
		t.Error(err)
		return
	}

	defer db.Close()

	if _, err = IngestJSONLd(db, d1, sampleData, false); err != nil {
		t.Error(err)
		return
	}

	if _, err = IngestJSONLd(db, d2, sampleData2, false); err != nil {
		t.Error(err)
		return
	}

	db.Log()

	list := db.List(d2)
	defer list.Close()

	for ; list.Valid(); list.Next() {
		log.Println(list.URI())
	}
}

func testQuery(query string, data map[string]interface{}) (db *styx, pattern []*ld.Quad, err error) {
	// Remove old db
	fmt.Println("removing path", DefaultPath)
	err = os.RemoveAll(DefaultPath)
	if err != nil {
		return
	}

	db, err = OpenDB(DefaultPath, tag)
	if err != nil {
		return
	}

	for uri, d := range data {
		_, err = IngestJSONLd(db, uri, d, false)
		if err != nil {
			return
		}
	}

	db.Log()

	var queryData map[string]interface{}
	err = json.Unmarshal([]byte(query), &queryData)
	if err != nil {
		return
	}

	proc := ld.NewJsonLdProcessor()
	opts := ld.NewJsonLdOptions("")
	opts.ProduceGeneralizedRdf = true

	dataset, err := proc.ToRDF(queryData, opts)
	if err != nil {
		return
	}

	d, _ := dataset.(*ld.RDFDataset)
	pattern = d.GetQuads("@default")
	return
}

func TestSPO(t *testing.T) {
	db, pattern, err := testQuery(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@id": "http://people.com/jane",
	"name": { }
}`, map[string]interface{}{d1: sampleData})
	defer db.Close()
	if err != nil {
		t.Error(err)
		return
	}

	cursor, err := db.Query(pattern, nil, nil)

	if err != nil {
		t.Error(err)
		return
	}

	defer cursor.Close()
	for d := cursor.Domain(); err == nil; d, err = cursor.Next(nil) {
		for _, b := range d {
			fmt.Printf("%s: %s\n", b.Attribute, cursor.Get(b).GetValue())
		}
		fmt.Println("---")
	}
}

func TestOPS(t *testing.T) {
	db, pattern, err := testQuery(`{
	"@context": { "@vocab": "http://schema.org/" },
	"name": "Jane Doe"
}`, map[string]interface{}{d1: sampleData})
	defer db.Close()
	if err != nil {
		t.Error(err)
		return
	}

	cursor, err := db.Query(pattern, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}
	defer cursor.Close()

	for d := cursor.Domain(); err == nil; d, err = cursor.Next(nil) {
		for _, b := range d {
			fmt.Printf("%s: %s\n", b.Attribute, cursor.Get(b).GetValue())
		}
		fmt.Println("---")
	}
}

func TestSimpleQuery(t *testing.T) {
	db, pattern, err := testQuery(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "Person",
	"birthDate": { }
}`, map[string]interface{}{d1: sampleData})
	defer db.Close()
	if err != nil {
		t.Error(err)
		return
	}

	cursor, err := db.Query(pattern, nil, nil)
	if err != nil {
		t.Error(err)
		return
	}

	for d := cursor.Domain(); err == nil; d, err = cursor.Next(nil) {
		for _, b := range d {
			fmt.Printf("%s: %s\n", b.Attribute, cursor.Get(b).GetValue())
		}
		fmt.Println("---")
	}
}

func TestSimpleQuery2(t *testing.T) {
	db, pattern, err := testQuery(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@type": "Person",
	"knows": {
		"name": "Jane Doe"
	}
}`, map[string]interface{}{d1: sampleData})
	defer db.Close()
	if err != nil {
		t.Error(err)
		return
	}

	cursor, err := db.Query(pattern, nil, nil)
	defer cursor.Close()
	if err != nil {
		t.Error(err)
		return
	}

	for d := cursor.Domain(); err == nil; d, err = cursor.Next(nil) {
		for _, b := range d {
			fmt.Printf("%s: %s\n", b.Attribute, cursor.Get(b).GetValue())
		}
		fmt.Println("---")
	}
}

func TestDomainQuery(t *testing.T) {
	db, pattern, err := testQuery(`{
	"@context": { "@vocab": "http://schema.org/" },
	"@id": "_:b0",
	"name": { "@id": "_:b1" }
}`, map[string]interface{}{d1: sampleData, d2: sampleData2})
	defer db.Close()
	if err != nil {
		t.Error(err)
		return
	}

	node := ld.NewBlankNode("_:b0")
	domain := []*ld.BlankNode{node}
	cursor, err := db.Query(pattern, domain, nil)
	if cursor != nil {
		defer cursor.Close()
		if err != nil {
			t.Error(err)
			return
		}
		for d := cursor.Domain(); err == nil; d, err = cursor.Next(node) {
			for _, b := range d {
				fmt.Printf("%s: %s\n", b.Attribute, cursor.Get(b).GetValue())
			}
			fmt.Println("---")
		}
	}
}

func TestIndexQuery(t *testing.T) {
	db, pattern, err := testQuery(`{
		"@context": { "@vocab": "http://schema.org/" },
		"@id": "_:b0",
		"name": { "@id": "_:b1" }
	}`, map[string]interface{}{d1: sampleData, d2: sampleData2})
	defer db.Close()
	if err != nil {
		t.Error(err)
		return
	}

	domain := []*ld.BlankNode{
		ld.NewBlankNode("_:b0"),
		ld.NewBlankNode("_:b1"),
	}
	index := []ld.Node{
		ld.NewIRI("http://example.com/d1#_:b1"),
		ld.NewLiteral("Johnny Doe", "", ""),
	}
	cursor, err := db.Query(pattern, domain, index)
	if err == ErrEndOfSolutions {
		log.Println("No solutions")
	} else if err != nil {
		t.Error(err)
		return
	} else {
		defer cursor.Close()
		for d := cursor.Domain(); err == nil; d, err = cursor.Next(nil) {
			for _, b := range d {
				fmt.Printf("%s: %s\n", b.Attribute, cursor.Get(b).GetValue())
			}
			fmt.Println("---")
		}
	}
}
