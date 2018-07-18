package fortysix

import (
	proto "github.com/golang/protobuf/proto"
	base58 "github.com/mr-tron/base58/base58"
	leveldb "github.com/syndtr/goleveldb/leveldb"
)

const path = "/Users/joel/Documents/Projects/fortysix/leveldb"
const length = 8
const hashLength = 49

// DB is a length-six slice of LevelDB database structs.
type DB []*leveldb.DB

// Quad is an RDF triple tagged with the base58-encoded CID of its source assertion
type Quad struct {
	triple []string
	cid    string
}

/*
How to index permutations lexicographically?
In general, this is hard, although there are linear-time algorithms to do it.
But when we fix a length (three) we can just look for patterns manually.
In this case, we notice that the base-3 value of the first two elements of the permutation
*almost* make an incrementing sequence (it skips 4 because 4 = 11 in base 3).
But this is a constant-time fix, so it's good enough.
index  permutation  r
    0    [0, 1, 2]  1 <- this number is r.
    1    [0, 2, 1]  2    r % 3 = b.
    2    [1, 0, 2]  3    r = a + (3 * b)
    3    [1, 2, 0]  5
    4    [2, 0, 1]  6
    5    [2, 1, 0]  7
*/

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
	key := append([]byte(a), []byte(b)...)
	label, _ := base58.Decode(quad.cid)
	value := Value{Value: quad.triple[p[2]], Cid: label}
	return key, value
}

// here p []int is any permutation of [0, 1, 2].
// [0, 1, 2] corresponds to "SPO"-indexing, etc.
func insert(quad Quad, db DB) {
	for i := 0; i < 6; i++ {
		p := index(i)
		d := db[i]
		key, value := splitQuad(quad, p)
		has, _ := d.Has(key, nil)
		if has {
			old, _ := d.Get(key, nil)
			entry := Entry{}
			_ = proto.Unmarshal(old, &entry)
			entry.Values = append(entry.Values, &value) // TODO: sort entries
			bytes, _ := proto.Marshal(&entry)
			d.Put(key, bytes, nil)
		} else {
			entry := Entry{Values: []*Value{&value}}
			bytes, _ := proto.Marshal(&entry)
			d.Put(key, bytes, nil)
		}
	}
}

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
