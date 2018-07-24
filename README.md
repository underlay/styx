# styx

Home-grown quadstore inspired by [Hexastore](https://dl.acm.org/citation.cfm?id=1453965), tailored for use in the Underlay. 

## Usage

### Create a store
```go
path := "./leveldb"
store := OpenStore(path)
```

### Insert an N-Quad
```go
triple := Triple{"alice", "likes", "pizza"}
cid := "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM" // must be b58-encoded
quad := Quad{Triple: triple, Cid: cid}

store.Insert(quad)
```

### Index an incomplete triple
```go
// "blank nodes" begin with "_:"
store.IndexTriple(Triple{"alice", "likes", "_:foo"})
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]

store.IndexTriple(Triple{"alice", "_:bar", "pizza"})
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]

store.IndexTriple(Triple{"_:baz", "likes", "pizza"})
// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
```

### Ingest JSON-LD documents
```go
var doc = map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
	},
	"@graph": []interface{}{
		map[string]interface{}{
			"@type":    "Movie",
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

// You have to supply a b58-encoded "label" for every document.
// This should the document's CID, but we can fake one if we need to.
store.Ingest(doc, "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM")
```

### Query with JSON-LD
```go
// `Variable` == "http://underlay.mit.edu/query#"
// It's a special namespace to label variables.
query := map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
		"$":      Variable,
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
director is Alfred Hitchcock
city is Leytonstone
population is 12879
*/
```

### Query with property path
```go
root := map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
	},
	"name": "Vertigo",
}

path := []string{"director", "hometown", "population"}

branch, _ := store.ResolvePath(root, path)

for variable, value := range branch.frame {
	if isVariable(variable) {
		name := variable[len(Variable):]
		fmt.Println(name, "is", value.Value)
	}
}
/*
result is 12879
*/
```
