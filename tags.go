package styx

import (
	"net/url"
	"strings"
)

// A TagScheme is an interface for testing whether a given URI is a dataset URI or not
type TagScheme interface {
	Test(uri string) bool
	Parse(uri string) (tag string, fragment string)
}

type nilTagScheme struct{}

func (nts nilTagScheme) Test(uri string) bool                    { return false }
func (nts nilTagScheme) Parse(uri string) (tag, fragment string) { return }

type prefixTagScheme string

// NewPrefixTagScheme creates a tag scheme that tests for the given prefix
func NewPrefixTagScheme(prefix string) TagScheme { return prefixTagScheme(prefix) }

func (pts prefixTagScheme) Test(uri string) bool {
	return strings.Index(uri, string(pts)) == 0 && strings.Index(uri, "#") >= len(pts)
}

func (pts prefixTagScheme) Parse(uri string) (tag, fragment string) {
	u, err := url.Parse(uri)
	if err == nil {
		fragment = u.Fragment
		tag = strings.TrimSuffix(uri, "#"+fragment)
	}
	return
}
