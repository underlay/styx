package styx

import (
	"fmt"
	"testing"
)

func TestThing(t *testing.T) {
	const path = "./leveldb"
	store := OpenStore(path)

	triple := Triple{"joel", "likes", "pizza"}
	cid := "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM"
	quad := Quad{triple, cid}
	Insert(quad, store)
	// yay! now query
	fmt.Println(indexTriple(0, "likes", "pizza", store))
	fmt.Println(indexTriple(1, "pizza", "joel", store))
	fmt.Println(indexTriple(2, "joel", "likes", store))
}
