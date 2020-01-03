package types

import (
	ld "github.com/underlay/json-gold/ld"
)

// Cursor is the important thing here
type Cursor interface {
	Graph() []*ld.Quad
	Get(node *ld.BlankNode) ld.Node
	Domain() []*ld.BlankNode
	Index() []ld.Node
	Next(node *ld.BlankNode) ([]*ld.BlankNode, error)
	Seek(index []ld.Node) error
	Close()
}
