package styx

import (
	proto "github.com/golang/protobuf/proto"
	base58 "github.com/mr-tron/base58/base58"
	leveldb "github.com/syndtr/goleveldb/leveldb"
)

// Store is a length-six array of LevelDB database structs.
type Store [6]*leveldb.DB

// Triple is your regular RDF subject-predicate-object triple.
type Triple [3]string

// Quad is an RDF triple tagged with the base58-encoded CID of its source assertion
type Quad struct {
	triple Triple
	cid    string
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

map for db indices:
---------------
j  i          p
0  3  [1, 2, 0]
1  4  [2, 0, 1]
2  0  [0, 1, 2]
so i = (j + 3) % 5
*/

// Here p []int is any permutation of {0, 1, 2}.
// So (p = 0) == [0, 1, 2] corresponds to "SPO"-indexing, etc.
func deindex(p []int) int {
	a := p[0]
	b := p[1]
	r := a + (3 * b)
	if r > 4 {
		r = r - 1
	}
	return r - 1
}

func index(i int) []int {
	a := int(i / 2)
	if i < 3 {
		i = i + 1
	} else {
		i = i + 2
	}
	b := i - (3 * a)
	c := 3 - (a + b)
	return []int{a, b, c}
}

func splitQuad(quad Quad, p []int) ([]byte, Value) {
	a := quad.triple[p[0]]
	b := quad.triple[p[1]]
	key := Key{A: a, B: b}
	label, _ := base58.Decode(quad.cid)
	value := Value{Value: quad.triple[p[2]], Cid: label}
	bytes, _ := proto.Marshal(&key)
	return bytes, value
}

// Insert a quad into the store
func Insert(quad Quad, store Store) {
	for i := 0; i < 6; i++ {
		p := index(i)
		db := store[i]
		key, value := splitQuad(quad, p)
		has, _ := db.Has(key, nil)
		if has {
			old, _ := db.Get(key, nil)
			entry := Entry{}
			_ = proto.Unmarshal(old, &entry)
			entry.Values = append(entry.Values, &value) // TODO: sort entries
			bytes, _ := proto.Marshal(&entry)
			db.Put(key, bytes, nil)
		} else {
			entry := Entry{Values: []*Value{&value}}
			bytes, _ := proto.Marshal(&entry)
			db.Put(key, bytes, nil)
		}
	}
}

func indexTriple(j int, a string, b string, store Store) []Quad {
	key := Key{A: a, B: b}
	bytes, _ := proto.Marshal(&key)
	i := (j + 3) % 5
	has, _ := store[i].Has(bytes, nil)
	if has {
		result, _ := store[i].Get(bytes, nil)
		var entry Entry
		_ = proto.Unmarshal(result, &entry)
		length := len(entry.Values)
		results := make([]Quad, length)
		for v := 0; v < length; v++ {
			cid := base58.Encode(entry.Values[v].Cid)
			value := entry.Values[v].Value
			triple := Triple{}
			triple[j] = value
			triple[(j+1)%3] = a
			triple[(j+2)%3] = b
			results[v] = Quad{triple, cid}
		}
		return results
	}
	return []Quad{}
}

// Index a simple query into matching quads
// func IndexTriple(triple Triple, store Store) []Quad {
// 	// An empty query deserves an empty response
// 	if len(triple[0]+triple[1]+triple[2]) == 0 {
// 		return []Quad{}
// 	}

// 	if triple[0] == "" {
// 		i = 3
// 	} else if triple[1] == "" {
// 		i = 4
// 	} else if triple[2] == "" {
// 		i = 0
// 	} else {
// 		// Panic!
// 		log.Fatalln("Invalid triple index")
// 	}
// 	p := index(i)
// 	return []Quad{}
// }

// func parseEntry(entry []byte, db DB) []Value {
// 	unit := length + hashLength
// 	count := len(entry) / unit
// 	values := make([]Value, count)
// 	for i := 0; i < count; i++ {
// 		start := i * unit
// 		middle := start + length
// 		end := start + unit
// 		id := entry[start:middle]
// 		value, _ := getRef(id, db)
// 		label := string(entry[middle:end])
// 		values[i] = Value{value, label}
// 	}
// 	return values
// }

// func queryEntry(index []string, db DB) []Value {
// 	var p int
// 	var key []byte
// 	for a := 0; a < 3; a++ {
// 		if index[a] == "?" {
// 			if a == 0 {
// 				p = 3
// 			} else if a == 1 {
// 				p = 4
// 			} else if a == 2 {
// 				p = 0
// 			}
// 			b := (a + 1) % 3
// 			c := (a + 2) % 3
// 			bs, _ := getID(index[b], db)
// 			cs, _ := getID(index[c], db)
// 			key = append(bs, cs...)
// 			break
// 		}
// 	}
// 	val, _ := db.p[p].Get(key, nil)
// 	return parseEntry(val, db)
// }

var dbNames = [6]string{"spo", "sop", "pso", "pos", "osp", "ops"}

// OpenStore of LevelDB databases, creating them if necessary
func OpenStore(path string) Store {
	store := Store{}
	for i := 0; i < 6; i++ {
		store[i], _ = leveldb.OpenFile(path+"/"+dbNames[i], nil)
	}
	return store
}
