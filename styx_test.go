package styx

import (
	"fmt"
	"testing"
)

func TestThing(t *testing.T) {
	const path = "./leveldb"
	store := OpenStore(path)
	fmt.Println("wow", store)
}
