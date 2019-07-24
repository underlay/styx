package db

import (
	cid "github.com/ipfs/go-cid"
	ld "github.com/piprate/json-gold/ld"
	types "github.com/underlay/styx/types"
)

func permuteMajor(permutation uint8, s, p, o []byte) ([]byte, []byte, []byte) {
	if permutation == 0 {
		return s, p, o
	} else if permutation == 1 {
		return p, o, s
	} else {
		return o, s, p
	}
}

func permuteMinor(permutation uint8, s, p, o []byte) ([]byte, []byte, []byte) {
	if permutation == 0 {
		return s, o, p
	} else if permutation == 1 {
		return p, s, o
	} else {
		return o, p, s
	}
}

type DocumentStore = func(data []byte) (cid.Cid, error)

// GetDatasetOptions returns JsonLdOptions for parsing a document into a dataset
func GetDatasetOptions(loader ld.DocumentLoader) *ld.JsonLdOptions {
	options := ld.NewJsonLdOptions("")
	options.ProcessingMode = ld.JsonLd_1_1
	options.DocumentLoader = loader
	options.UseNativeTypes = true
	options.CompactArrays = true
	return options
}

// GetStringOptions returns JsonLdOptions for serializing a dataset into a string
func GetStringOptions(loader ld.DocumentLoader) *ld.JsonLdOptions {
	options := ld.NewJsonLdOptions("")
	options.ProcessingMode = ld.JsonLd_1_1
	options.DocumentLoader = loader
	options.Algorithm = types.Algorithm
	options.Format = types.Format
	return options
}
