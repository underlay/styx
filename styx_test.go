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
	quad := Quad{Triple: triple, Cid: cid}
	Insert(quad, store)
	// yay! now query
	fmt.Println(minorIndex(0, "pizza", "joel", store))
	fmt.Println(minorIndex(1, "likes", "pizza", store))
	fmt.Println(minorIndex(2, "joel", "likes", store))
}
