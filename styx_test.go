package styx

import (
	"fmt"
	"testing"
)

const path = "./leveldb"

var query = map[string]interface{}{
	"@context": map[string]interface{}{
		"@vocab": "http://schema.org/",
		"$":      "http://underlay.mit.edu/query#",
	},
	"@type": "Movie",
	"name":  "Vertigo",
	"director": map[string]interface{}{
		"hometown": map[string]interface{}{
			"population": map[string]interface{}{"@id": "$:population"},
			"name":       map[string]interface{}{"@id": "$:name"},
		},
	},
}

// func TestQuery(t *testing.T) {
// 	ResolveQuery(query)
// }

func TestIndex(t *testing.T) {

	store := OpenStore(path)

	triple := Triple{"alice", "likes", "pizza"}
	cid := "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM"
	quad := Quad{Triple: triple, Cid: cid}

	// insert!
	store.Insert(quad)

	// yay! now query.
	fmt.Println(store.IndexTriple(Triple{"alice", "likes", ""}))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
	fmt.Println(store.IndexTriple(Triple{"alice", "", "pizza"}))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
	fmt.Println(store.IndexTriple(Triple{"", "likes", "pizza"}))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
}
