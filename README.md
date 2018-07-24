# styx

Home-grown quadstore inspired by [Hexastore](https://dl.acm.org/citation.cfm?id=1453965), tailored for use in the Underlay. 

```go
store := OpenStore(path)
triple := Triple{"alice", "likes", "pizza"}
cid := "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM"
quad := Quad{Triple: triple, Cid: cid}

store.Insert(quad)

fmt.Println(store.IndexTriple(Triple{"alice", "likes", ""}))
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
fmt.Println(store.IndexTriple(Triple{"alice", "", "pizza"}))
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
fmt.Println(store.IndexTriple(Triple{"", "likes", "pizza"}))
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]

// Can also ingest JSON-LD documents
var doc = map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
	},
	"@graph": []interface{}{
		map[string]interface{}{
			"@type":    "http://schema.org/Movie",
			"name":     "Vertigo",
			"director": map[string]interface{}{"@id": "_:n0"},
		},
		map[string]interface{}{
			"@id":   "_:n0",
			"@type": "Person",
			"name":  "Alfred Hitchcock",
			"hometown": map[string]interface{}{
				"population": "12879",
				"name":       "Leytonstone",
			},
		},
	},
}

store.Ingest(assertion, "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM")

// You can also query with JSON-LD
var query = map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
		"$":      "http://underlay.mit.edu/query#",
	},
	"@type": "Movie",
	"name":  "Vertigo",
	"director": map[string]interface{}{
		"name": map[string]interface{}{"@id": "$:director"},
		"hometown": map[string]interface{}{
			"population": map[string]interface{}{"@id": "$:population"},
			"name":       map[string]interface{}{"@id": "$:city"},
		},
	},
}

branch, _ := store.ResolveQuery(query)
for variable, value := range branch.frame {
  if isVariable(variable) {
    // value.Value is the string value
    // value.Source is an array of n-quads that support the value
    fmt.Println(variable, "is", value.Value)
  }
}
/*
http://underlay.mit.edu/query#director is Alfred Hitchcock
http://underlay.mit.edu/query#city is Leytonstone
http://underlay.mit.edu/query#population is 12879
*/
```
