package styx

import (
	"net/url"
	"strings"

	cid "github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
	"github.com/piprate/json-gold/ld"
)

// DefaultShellAddress is the default shell address
const DefaultShellAddress = "localhost:5001"

// IPFSDocumentLoader is an implementation of DocumentLoader for ipfs:// and dweb:/ipfs/ URIs
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

	// I'm pretty sure we shouldn't do anything with contextURL.
	var contextURL string

	var origin, path string
	if parsedURL.Scheme == "ipfs" {
		origin = parsedURL.Host
		path = parsedURL.Path
	} else if parsedURL.Scheme == "dweb" && parsedURL.Path[:6] == "/ipfs/" {
		index := strings.Index(parsedURL.Path[6:], "/")
		if index == -1 {
			index = len(parsedURL.Path)
		} else {
			index += 6
		}
		origin = parsedURL.Path[6:index]
		path = parsedURL.Path[index:]
	}

	if c, err := cid.Decode(origin); err != nil {
		return nil, err
	} else if c.Version() == 0 {
		result, err := dl.shell.Cat(origin + path)
		if err != nil {
			return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
		}
		defer result.Close()
		document, err := ld.DocumentFromReader(result)
		if err != nil {
			return nil, err
		}
		return &ld.RemoteDocument{DocumentURL: u, Document: document, ContextURL: contextURL}, nil
	} else if c.Type() == cid.DagCBOR {
		var document interface{}
		err := dl.shell.DagGet(origin+path, &document)
		if err != nil {
			return nil, err
		}
		return &ld.RemoteDocument{DocumentURL: u, Document: document, ContextURL: contextURL}, nil
	} else {
		err := "Unsupported URI scheme: " + parsedURL.Scheme
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}
}

// NewIPFSDocumentLoader creates a new instance of IPFSDocumentLoader
func NewIPFSDocumentLoader(shell *ipfs.Shell) *IPFSDocumentLoader {
	rval := &IPFSDocumentLoader{shell: shell}

	if rval.shell == nil {
		rval.shell = ipfs.NewShell(DefaultShellAddress)
	}
	return rval
}
