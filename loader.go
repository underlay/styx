package styx

import (
	"io"
	"net/url"

	ipfs "github.com/ipfs/go-ipfs-api"
	"github.com/piprate/json-gold/ld"
)

// IPFSDocumentLoader is an implementation of DocumentLoader for dweb URIs
type IPFSDocumentLoader struct {
	shell *ipfs.Shell
}

// LoadDocument returns a RemoteDocument containing the contents of the
// JSON-LD resource from the given URL.
func (dl *IPFSDocumentLoader) LoadDocument(u string) (*ld.RemoteDocument, error) {
	parsedURL, err := url.Parse(u)
	if err != nil {
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}
	var reader io.Reader
	var contextURL string
	protocol := parsedURL.Scheme
	protocolError := "unsupported URI scheme: " + protocol
	if protocol == "ipfs" {
		path := parsedURL.Host + parsedURL.Path
		result, err := dl.shell.Cat(path)
		if err != nil {
			return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
		}
		defer result.Close()
		reader = result
	} else if protocol == "dweb" {
		root := parsedURL.Path[:6]
		rest := parsedURL.Path[6:]
		if root == "/ipfs/" {
			result, err := dl.shell.Cat(rest)
			if err != nil {
				return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
			}
			defer result.Close()
			reader = result
		} else if root == "/ipld/" {
			var document interface{}
			err := dl.shell.DagGet(rest, &document)
			if err != nil {
				return nil, err
			}
			return &ld.RemoteDocument{DocumentURL: u, Document: document, ContextURL: contextURL}, nil
		} else {
			return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, protocolError)
		}
	} else {
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, protocolError)
	}
	document, err := ld.DocumentFromReader(reader)
	if err != nil {
		return nil, err
	}
	return &ld.RemoteDocument{DocumentURL: u, Document: document, ContextURL: contextURL}, nil
}

// NewIPFSDocumentLoader creates a new instance of IPFSDocumentLoader
func NewIPFSDocumentLoader(shell *ipfs.Shell) *IPFSDocumentLoader {
	rval := &IPFSDocumentLoader{shell: shell}

	if rval.shell == nil {
		rval.shell = ipfs.NewShell("localhost:5001")
	}
	return rval
}
