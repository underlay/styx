package styx

import (
	fmt "fmt"

	proto "github.com/golang/protobuf/proto"
	base58 "github.com/mr-tron/base58/base58"
	ld "github.com/piprate/json-gold/ld"
	leveldb "github.com/syndtr/goleveldb/leveldb"
)

// Store is a six-element table of LevelDB database structs.
type Store [2][3]*leveldb.DB

// Triple is your regular RDF subject-predicate-object triple.
type Triple [3]string

// Quad is an RDF triple tagged with the base58-encoded CID of its source assertion
type Quad struct {
	Triple Triple
	Cid    string
}

func index(p int) (int, int, int) {
	a := p
	b := (a + 1) % 3
	c := (a + 2) % 3
	return a, b, c
}

func focus(i int) (p int) {
	return (i + 1) % 3
}

// Major indices are indexed by a single element of a triple,
// and map the single element to two arrays, one for each of
// the other two elements.
// So there are three major indices:
// p = 0 maps <subject> keys to a {predicate[] object[]} value
// p = 1 maps <predicate> keys to a {object[] subject[]} value
// p = 2 maps <object> keys to a {subject[] predicate[]} value
func insertMajor(p int, quad Quad, db *leveldb.DB) {
	// Major key
	a, b, c := index(p)
	key := []byte(quad.Triple[a])
	has, _ := db.Has(key, nil)
	majorValue := MajorValue{}
	if has {
		value, _ := db.Get(key, nil)
		_ = proto.Unmarshal(value, &majorValue)
		majorValue.B = append(majorValue.B, quad.Triple[b])
		majorValue.C = append(majorValue.C, quad.Triple[c])
	} else {
		B := []string{quad.Triple[b]}
		C := []string{quad.Triple[c]}
		majorValue = MajorValue{B: B, C: C}
	}
	bytes, _ := proto.Marshal(&majorValue)
	_ = db.Put(key, bytes, nil)
}

// Minor indices are indexed by two elements of a triple.
// There are three minor indices; one for every rotation of [0 1 2].
// So  p = 0 maps <subject|predicate> keys to {object label}[] values,
//     p = 1 maps <predicate|object> keys to {subject label}[] values,
// and p = 2 maps <object|subject> keys to {predicate label}[] values.
func insertMinor(p int, quad Quad, db *leveldb.DB) {
	// Minor key
	a, b, c := index(p)
	fmt.Println("from index", p, "got", a, b, c)
	minorKey := MinorKey{A: quad.Triple[a], B: quad.Triple[b]}
	fmt.Println("inserting minor key:", quad.Triple[a], quad.Triple[b])
	key, _ := proto.Marshal(&minorKey)
	has, _ := db.Has(key, nil)
	cid, _ := base58.Decode(quad.Cid)
	entry := MinorEntry{C: quad.Triple[c], Cid: cid}
	minorValue := MinorValue{}
	if has {
		value, _ := db.Get(key, nil)
		_ = proto.Unmarshal(value, &minorValue)
		minorValue.Entries = append(minorValue.Entries, &entry)
	} else {
		entries := []*MinorEntry{&entry}
		minorValue = MinorValue{Entries: entries}
	}
	bytes, _ := proto.Marshal(&minorValue)
	_ = db.Put(key, bytes, nil)
}

// Insert a quad into the store
func (store Store) Insert(quad Quad) {
	for j := 0; j < 3; j++ {
		insertMajor(j, quad, store[0][j])
		insertMinor(j, quad, store[1][j])
	}
}

// Ingest a JSON-LD document
func (store Store) Ingest(doc interface{}, cid string) {
	processor := ld.NewJsonLdProcessor()
	options := ld.NewJsonLdOptions("")
	api := ld.NewJsonLdApi()
	expanded, _ := processor.Expand(doc, options)
	dataset, _ := api.ToRDF(expanded, options)
	for _, quads := range dataset.Graphs {
		for _, quad := range quads {
			triple := parseQuad(quad)
			for i, value := range triple {
				if len(value) > 2 && value[:2] == "_:" {
					triple[i] = cid + value[1:]
				}
				quad := Quad{triple, cid}
				store.Insert(quad)
			}
		}
	}
}

func (store Store) minorIndex(p int, A string, B string) []Quad {
	a, b, c := index(p)
	fmt.Println("from p", p, "got a", a, "and b", b)
	minorKey := MinorKey{A: A, B: B}
	key, _ := proto.Marshal(&minorKey)
	has, _ := store[1][p].Has(key, nil)
	results := []Quad{}
	if has {
		value, _ := store[1][p].Get(key, nil)
		minorValue := MinorValue{}
		proto.Unmarshal(value, &minorValue)
		length := len(minorValue.Entries)
		results = make([]Quad, length)
		for k := 0; k < length; k++ {
			entry := minorValue.Entries[k]
			triple := Triple{}
			triple[a] = A
			triple[b] = B
			triple[c] = entry.C
			cid := base58.Encode(entry.Cid)
			results[k] = Quad{Triple: triple, Cid: cid}
		}
	}
	return results
}

func isEmpty(value string) bool {
	// The empty string OR blank node IDs
	return value == "" || (len(value) > 2 && value[:2] == "_:")
}

// IndexTriple takes a triple with *exactly one empty-string element*.
// I'm not responsible for its behaviour otherwise :-/
func (store Store) IndexTriple(triple Triple) []Quad {
	var p int
	if isEmpty(triple[0]) {
		p = 1
	} else if isEmpty(triple[1]) {
		p = 2
	} else if isEmpty(triple[2]) {
		p = 0
	}
	a, b, _ := index(p)
	A := triple[a]
	B := triple[b]
	return store.minorIndex(p, A, B)
}

var dbNames = [6]string{"s-major", "p-major", "o-major", "s-minor", "p-minor", "o-minor"}

// OpenStore of LevelDB databases, creating them if necessary
func OpenStore(path string) Store {
	store := Store{}
	for k := 0; k < 6; k++ {
		name := path + "/" + dbNames[k]
		db, _ := leveldb.OpenFile(name, nil)
		store[k/3][k%3] = db
	}
	return store
}
