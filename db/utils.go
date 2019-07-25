package db

import (
	"context"
	"io"

	cid "github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
	files "github.com/ipfs/go-ipfs-files"
	core "github.com/ipfs/interface-go-ipfs-core"
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

// DocumentStore is a function that turns bytes into CIDs (and probably pins them too)
type DocumentStore = func(reader io.Reader) (cid.Cid, error)

// MakeShellDocumentStore wraps the HTTP API interface
func MakeShellDocumentStore(sh *ipfs.Shell) DocumentStore {
	return func(reader io.Reader) (cid.Cid, error) {
		hash, err := sh.Add(reader)
		if err != nil {
			return cid.Undef, err
		}
		return cid.Parse(hash)
	}
}

// MakeAPIDocumentStore wraps the native CoreAPI interface
func MakeAPIDocumentStore(unixfsAPI core.UnixfsAPI) DocumentStore {
	return func(reader io.Reader) (cid.Cid, error) {
		file := files.NewReaderFile(reader)
		path, err := unixfsAPI.Add(context.Background(), file)
		if err != nil {
			return cid.Undef, err
		}
		return path.Cid(), nil
	}
}

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
