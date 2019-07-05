package loader

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	cid "github.com/ipfs/go-cid"
	files "github.com/ipfs/go-ipfs-files"
	cbor "github.com/ipfs/go-ipld-cbor"
	core "github.com/ipfs/interface-go-ipfs-core"
	path "github.com/ipfs/interface-go-ipfs-core/path"
	ld "github.com/piprate/json-gold/ld"
)

// CoreDocumentLoader is an implementation of ld.DocumentLoader
// for ipfs:// and dweb:/ipfs/ URIs that an core.CoreAPI
type CoreDocumentLoader struct {
	api core.CoreAPI
}

// LoadDocument returns a RemoteDocument containing the contents of the
// JSON-LD resource from the given URL.
func (dl *CoreDocumentLoader) LoadDocument(uri string) (*ld.RemoteDocument, error) {
	parsedURL, err := url.Parse(uri)
	if err != nil {
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}

	// Don't do anything with contextURL.
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

func (dl *CoreDocumentLoader) loadDocumentIPLD(uri string, contextURL string, origin string) (*ld.RemoteDocument, error) {
	if c, err := cid.Decode(origin); err != nil {
		return nil, err
	} else if c.Type() == cid.DagCBOR {
		var document interface{}

		dagAPI := dl.api.Dag()
		node, err := dagAPI.Get(context.Background(), c)
		if err != nil {
			return nil, err
		}
		cborNode, isCborNode := node.(*cbor.Node)
		if !isCborNode {
			err := "Unsupported IPLD CID format: " + origin
			return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
		}
		data, err := cborNode.MarshalJSON()
		json.Unmarshal(data, &document)
		return &ld.RemoteDocument{DocumentURL: uri, Document: document, ContextURL: contextURL}, nil
	} else {
		err := "Unsupported IPLD CID format: " + origin
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}
}

func (dl *CoreDocumentLoader) loadDocumentIPFS(uri string, contextURL string, origin string, remainder string) (*ld.RemoteDocument, error) {
	if c, err := cid.Decode(origin); err != nil {
		return nil, err
	} else if c.Version() == 0 {
		unixFsAPI := dl.api.Unixfs()
		root := path.IpfsPath(c)
		tail := path.Join(root, remainder)
		ctx := context.Background()
		node, err := unixFsAPI.Get(ctx, tail)
		defer node.Close()
		if err != nil {
			return nil, err
		} else if file, isFile := node.(files.File); isFile {
			document, err := ld.DocumentFromReader(file)
			if err != nil {
				return nil, err
			}
			return &ld.RemoteDocument{DocumentURL: uri, Document: document, ContextURL: contextURL}, nil
		} else {
			return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, "Cannot load directory")
		}
	} else {
		err := "Invalid IPFS origin CID: " + origin
		return nil, ld.NewJsonLdError(ld.LoadingDocumentFailed, err)
	}
}

// NewCoreDocumentLoader creates a new instance of DwebDocumentLoader
func NewCoreDocumentLoader(api core.CoreAPI) *CoreDocumentLoader {
	return &CoreDocumentLoader{api}
}
