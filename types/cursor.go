package types

import (
	cid "github.com/ipfs/go-cid"
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

// Styx is a stupid interface
type Styx interface {
	Query(query []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (Cursor, error)
	Insert(c cid.Cid, dataset []*ld.Quad) error
	Delete(c cid.Cid, dataset []*ld.Quad) error
}
