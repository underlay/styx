# styx

> https://pkg.go.dev/github.com/underlay/styx

Styx is an experimental RDF database that works like a key/value store, where the keys are RDF IRI terms and the values are RDF datasets. It also exposes a _subgraph iterator_ interface that lets you iterate over all the subgraphs of the database that match a given pattern.

You can use Styx as a Go module by importing `github.com/underlay/styx`, or you can run the API daemon in `github.com/underlay/styx/api`, which exposes get/set/delete via GET, PUT, and DELETE requests, and subgraph iteration over a websocket RPC interface.
