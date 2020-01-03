package types

import (
	cid "github.com/ipfs/go-cid"
	ld "github.com/underlay/json-gold/ld"
)

// Styx is a stupid interface
type Styx interface {
	Query(query []*ld.Quad, domain []*ld.BlankNode, index []ld.Node) (Cursor, error)
	Insert(c cid.Cid, dataset []*ld.Quad) error
	Delete(c cid.Cid, dataset []*ld.Quad) error
	Close() error
	Log()
}
