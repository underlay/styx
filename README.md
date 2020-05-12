# styx

> https://pkg.go.dev/github.com/underlay/styx

Styx is an experimental RDF database that works like a key/value store, where the keys are RDF IRI terms and the values are RDF datasets. It also exposes a _subgraph iterator_ interface that lets you iterate over all the subgraphs of the database that match a given pattern.

You can use Styx as a Go module by importing `github.com/underlay/styx`. Alternatively, you can run in the background by building the package `github.com/underlay/styx/api`:

```
% cd api
% go build -o styx
% export STYX_PREFIX=http://example.com/
% ./styx
```

This will start an API server exposing get/set/delete via GET, PUT, and DELETE requests, and subgraph iteration over a websocket RPC interface.

Set the Styx database location by setting the `STYX_PATH` evironment variable. It will default to `/tmp/styx`.

Set the API port with `STYX_PORT`. It will default to `8086`.

You also need to set the `STYX_PREFIX` variable to a string like `http://...` that all of the keys you'll set will start with. For example, setting `STYX_PREFIX=http://example.com/` means that you'll be able to insert datasets with keys beginning with `http://example.com/`. It will default to `http://localhost:${STYX_PORT}`. You don't need this if you only ever use the default dataset.
