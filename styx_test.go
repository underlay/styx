package styx

import (
	fmt "fmt"
	"testing"
)

const path = "./leveldb"

var assertion = map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
		"$":      "http://underlay.mit.edu/query#",
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

var query = map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
		"$":      variable,
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

var query0 = map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
		"$":      variable,
	},
	"likes": map[string]interface{}{
		"@id": "$:foo",
	},
}

func TestQuery(t *testing.T) {
	store := OpenStore(path)
	store.Ingest(assertion, "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM")
	fmt.Println("Ingested assertion!")
	branch, _ := store.ResolveQuery(query)
	for variable, value := range branch.frame {
		fmt.Println(variable, "is", value.Value)
	}
	// fmt.Println(branch.frame, err)
}

func TestIndex(t *testing.T) {
	store := OpenStore(path)

	triple := Triple{"http://schema.org/alice", "http://schema.org/likes", "http://schema.org/pizza"}
	cid := "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM"
	quad := Quad{Triple: triple, Cid: cid}

	// insert!
	store.Insert(quad)

	// yay! now query.
	fmt.Println(store.IndexTriple(Triple{"http://schema.org/alice", "http://schema.org/likes", ""}))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
	fmt.Println(store.IndexTriple(Triple{"http://schema.org/alice", "", "http://schema.org/pizza"}))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
	fmt.Println(store.IndexTriple(Triple{"", "http://schema.org/likes", "http://schema.org/pizza"}))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
}
