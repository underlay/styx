package loader

import (
	"net/url"
	"strings"

	cid "github.com/ipfs/go-cid"
	ipfs "github.com/ipfs/go-ipfs-api"
	ld "github.com/piprate/json-gold/ld"
)

// DefaultShellAddress is the default shell address
const DefaultShellAddress = "localhost:5001"

// Compile-time type check
var _ ld.DocumentLoader = (*ShellDocumentLoader)(nil)

// ShellDocumentLoader is an implementation of ld.DocumentLoader
// for ipfs:// and dweb:/ipfs/ URIs that uses an ipfs.Shell
type ShellDocumentLoader struct {
	shell *ipfs.Shell
}

// LoadDocument returns a RemoteDocument containing the contents of the
// JSON-LD resource from the given URL.
func (dl *ShellDocumentLoader) LoadDocument(uri string) (*ld.RemoteDocument, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}

	// I'm pretty sure we shouldn't do anything with contextURL.
	var contextURL string

	var origin, path string
	if parsedURL.Scheme == "ipfs" {
		return dl.loadDocumentIPFS(uri, contextURL, parsedURL.Host, parsedURL.Path)
	} else if parsedURL.Scheme == "dweb" {
		if parsedURL.Path[:6] == "/ipfs/" {
			index := strings.Index(parsedURL.Path[6:], "/")
			if index == -1 {
				index = len(parsedURL.Path)
			} else {
				index += 6
			}
			origin = parsedURL.Path[6:index]
			path = parsedURL.Path[index:]
			return dl.loadDocumentIPFS(uri, contextURL, origin, path)
		} else if parsedURL.Path[:6] == "/ipld/" {
			return dl.loadDocumentIPLD(uri, contextURL, parsedURL.Path[6:])
		} else {
			err := "Unsupported dweb path: " + parsedURL.Path
			return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
		}
	} else {
		err := "Unsupported URI scheme: " + parsedURL.Scheme
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}
}

func (dl *ShellDocumentLoader) loadDocumentIPLD(uri string, contextURL string, origin string) (*ld.RemoteDocument, error) {
	if c, err := cid.Decode(origin); err != nil {
		return nil, err
	} else if c.Type() == cid.DagCBOR {
		var document interface{}
		err := dl.shell.DagGet(origin, &document)
		if err != nil {
			return nil, err
		}
		return &ld.RemoteDocument{DocumentURL: uri, Document: document, ContextURL: contextURL}, nil
	} else {
		err := "Unsupported IPLD CID format: " + origin
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}
}

func (dl *ShellDocumentLoader) loadDocumentIPFS(uri string, contextURL string, origin string, path string) (*ld.RemoteDocument, error) {
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
		return &ld.RemoteDocument{DocumentURL: uri, Document: document, ContextURL: contextURL}, nil
	} else {
		err := "Invalid IPFS origin CID: " + origin
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}
}

// NewShellDocumentLoader creates a new instance of DwebDocumentLoader
func NewShellDocumentLoader(shell *ipfs.Shell) *ShellDocumentLoader {
	loader := &ShellDocumentLoader{shell: shell}
	if loader.shell == nil {
		loader.shell = ipfs.NewShell(DefaultShellAddress)
	}
	return loader
}
