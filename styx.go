package styx

import (
	proto "github.com/golang/protobuf/proto"
	base58 "github.com/mr-tron/base58/base58"
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

/*
How to index permutations lexicographically?
In general, this is hard, although there are linear-time algorithms to do it.
But when we fix a length (three) we can just look for patterns manually.
In this case, we notice that the base-3 value of the first two elements of the permutation
*almost* make an incrementing sequence (it skips 4 because 4 = 11 in base 3).
But this is a constant-time fix, so it's good enough.
-----------------
i            p  r
0    [0, 1, 2]  1 <- this number is r.
1    [0, 2, 1]  2    r % 3 = b.
2    [1, 0, 2]  3    r = a + (3 * b)
3    [1, 2, 0]  5
4    [2, 0, 1]  6
5    [2, 1, 0]  7

We could also index permutations by rotation & flip (the generators of S_3)
This is more algebraically natural: let i range from 0 to 1 and j from 0 to 2:
ij
00   [0 1 2] a = (3 - i - j) % 3
01   [2 0 1] b = (a + 1) % 3
02   [1 2 0] c = (a + 2) % 3
10   [2 1 0] ...
11   [0 2 1]
12   [1 0 2]
-----------------
*/

func index(i int, j int) (int, int, int) {
	a := (3 - i - j) % 3
	b := (a + 1) % 3
	c := (a + 2) % 3
	return a, b, c
}

func insertMajor(j int, quad Quad, db *leveldb.DB) {
	// Major key
	a, b, c := index(0, j)
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

func insertMinor(j int, quad Quad, db *leveldb.DB) {
	// Minor key
	a, b, c := index(1, j)
	minorKey := MinorKey{A: quad.Triple[a], B: quad.Triple[b]}
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
func Insert(quad Quad, store Store) {
	for j := 0; j < 3; j++ {
		insertMajor(j, quad, store[0][j])
		insertMinor(j, quad, store[1][j])
	}
}

func minorIndex(j int, A string, B string, store Store) []Quad {
	a, b, c := index(1, j)
	minorKey := MinorKey{A: A, B: B}
	key, _ := proto.Marshal(&minorKey)
	has, _ := store[1][j].Has(key, nil)
	results := []Quad{}
	if has {
		value, _ := store[1][j].Get(key, nil)
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

/*
    a < p
    ^   ∨
x > y > z
    ^
    b
*/

// IndexTriple takes a triple with *exactly one empty-string element*. I'm not responsible for its behaviour otherwise :-/
func IndexTriple(triple Triple, store Store) []Quad {
	var j int
	if triple[0] == "" {
		j = 1
	} else if triple[1] == "" {
		j = 0
	} else if triple[2] == "" {
		j = 2
	} else {
		// Look for exact match?
		// no.
	}
	a, b, _ := index(1, j)
	A := triple[a]
	B := triple[b]
	return minorIndex(j, A, B, store)
}

var dbNames = [6]string{"smajor", "pmajor", "omajor", "sminor", "pminor", "bminor"}

// OpenStore of LevelDB databases, creating them if necessary
func OpenStore(path string) Store {
	store := Store{}
	for k := 0; k < 6; k++ {
		i := k / 3
		j := k % 3
		name := path + "/" + dbNames[k]
		db, _ := leveldb.OpenFile(name, nil)
		store[i][j] = db
	}
	return store
}
