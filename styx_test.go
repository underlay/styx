package styx

import (
	"fmt"
	"testing"
)

const path = "./leveldb"

func TestThing(t *testing.T) {

	store := OpenStore(path)

	triple := Triple{"alice", "likes", "pizza"}
	cid := "QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM"
	quad := Quad{Triple: triple, Cid: cid}

	// insert!
	Insert(quad, store)

	// yay! now query.
	fmt.Println(IndexTriple(Triple{"alice", "likes", ""}, store))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
	fmt.Println(IndexTriple(Triple{"alice", "", "pizza"}, store))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
	fmt.Println(IndexTriple(Triple{"", "likes", "pizza"}, store))
	// [{[alice likes pizza] QmfQ5QAjvg4GtA3wg3adpnDJug8ktA1BxurVqBD8rtgVjM}]
}
