package types

import (
	"fmt"
	"regexp"

	cid "github.com/ipfs/go-cid"
	multibase "github.com/multiformats/go-multibase"
)

const fragment = "(#(?:_:c14n\\d+)?)?"

var testUlURI = regexp.MustCompile(fmt.Sprintf("^ul:\\/ipfs\\/([a-zA-Z0-9]{59})%s$", fragment))
var testDwebURI = regexp.MustCompile("^dweb:\\/ipfs\\/([a-zA-Z0-9]{59})$")

// URI is an interface type for content-addressable semantic URIs
type URI interface {
	Parse(uri string) (c cid.Cid, fragment string)
	String(c cid.Cid, fragment string) (uri string)
	Test(uri string) bool
}

type ulURI struct{}

func (*ulURI) Parse(uri string) (c cid.Cid, fragment string) {
	if match := testUlURI.FindStringSubmatch(uri); match != nil {
		c, _ = cid.Decode(match[1])
		fragment = match[2]
	}
	return
}

func (*ulURI) String(c cid.Cid, fragment string) (uri string) {
	s, _ := c.StringOfBase(multibase.Base32)
	return fmt.Sprintf("ul:/ipfs/%s%s", s, fragment)
}

func (*ulURI) Test(uri string) bool {
	return testUlURI.Match([]byte(uri))
}

// UlURI are URIs that use a ul: protocol scheme
var UlURI URI = (*ulURI)(nil)

type dwebURI struct{}

func (*dwebURI) Parse(uri string) (c cid.Cid, fragment string) {
	if match := testDwebURI.FindStringSubmatch(uri); match != nil {
		c, _ = cid.Decode(match[1])
	}
	return
}

func (*dwebURI) String(c cid.Cid, fragment string) (uri string) {
	s, _ := c.StringOfBase(multibase.Base32)
	return fmt.Sprintf("dweb:/ipfs/%s%s", s, fragment)
}

func (*dwebURI) Test(uri string) bool {
	return testDwebURI.Match([]byte(uri))
}

// DwebURI are URIs that use a dweb: protocol scheme
var DwebURI URI = (*dwebURI)(nil)
