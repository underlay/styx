package types

import (
	ld "github.com/piprate/json-gold/ld"
)

// A Rule is important
type Rule struct {
	ld.Quad
	Body []*ld.Quad
}
