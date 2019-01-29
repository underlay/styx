# styx

Home-grown quadstore inspired by [Hexastore](https://dl.acm.org/citation.cfm?id=1453965), tailored for use in the Underlay.

## What's going on?

"Hexastore" is the silly idea that if you really care about your triples, you shouldn't just insert them once: you should actually insert them six times - one for each permutation of the three elements. And even better than regular hexastore, Styx performs 12 (twelve!) writes for every triple you want to insert!

```golang
// Open an IPFS Shell
sh := ipfs.NewShell("localhost:5001")

// Open a Badger database
path := "/tmp/badger"
opts := badger.DefaultOptions
opts.Dir = path
opts.ValueDir = path

db, err := badger.Open(opts)

// Ingest some data as JSON-LD
var data map[string]interface{}
var dataBytes = []byte(`{
  "@context": { "@vocab": "http://schema.org/" },
  "@graph": [
    {
      "@type": "Person",
      "name": "Joel",
      "birthDate": "1996-02-02",
      "children": { "@id": "http://people.com/liljoel" }
    },
    {
      "@id": "http://people.com/liljoel",
      "@type": "Person",
      "name": "Little Joel",
      "birthDate": "2030-11-10"
    }
  ]
}`)

json.Unmarshal(dataBytes, &data)
Ingest(data, db, sh)

// Query by subgraph pattern
var query map[string]interface{}
var queryBytes = []byte(`{
  "@context": {
    "@vocab": "http://schema.org/",
    "parent": {
      "@reverse": "children"
    }
  },
  "@type": "Person",
  "birthDate": {},
  "parent": {
    "name": "Joel"
  }
}`)

json.Unmarshal(queryBytes, &query)
Query(query, func(result interface{}) error {
  // The result will be framed by the query,
  // as per https://w3c.github.io/json-ld-framing
  bytes, _ := json.MarshalIndent(result, "", "\t")
  fmt.Println(string(bytes))
  return nil
}, db, sh)
```

When ingested, every JSON-LD document is first [normalized as an RDF dataset](https://json-ld.github.io/normalization/spec/).

```
<http://people.com/liljoel> <http://schema.org/birthDate> "2030-11-10" .
<http://people.com/liljoel> <http://schema.org/name> "Little Joel" .
<http://people.com/liljoel> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .
_:c14n0 <http://schema.org/birthDate> "1996-02-02" .
_:c14n0 <http://schema.org/children> <http://people.com/liljoel> .
_:c14n0 <http://schema.org/name> "Joel" .
_:c14n0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Person> .
```

This (canonicalized) dataset has an IPFS CID of `QmWMwTL4GZSEsAaNYUo7Co24HkAkVCSdPgMwGJmrH5TwMC` (you can view it from [any gateway](https://gateway.underlay.store/ipfs/QmWMwTL4GZSEsAaNYUo7Co24HkAkVCSdPgMwGJmrH5TwMC) or in the [Underlay explorer](https://underlay.github.io/explore/#QmWMwTL4GZSEsAaNYUo7Co24HkAkVCSdPgMwGJmrH5TwMC)!). So when the query processor wants to reference a blank node from that dataset, it'll use a URI staring with `dweb:/ipfs/QmWMwTL4GZSEsAaNYUo7Co24HkAkVCSdPgMwGJmrH5TwMC`, plus a fragment identifier for the (canonicalized) blank node id.

```
{
  "@context": {
    "@vocab": "http://schema.org/",
    "parent": {
      "@reverse": "http://schema.org/children"
    }
  },
  "@id": "http://people.com/liljoel",
  "@type": "Person",
  "birthDate": "2030-11-10",
  "parent": {
    "@id": "dweb:/ipfs/QmWMwTL4GZSEsAaNYUo7Co24HkAkVCSdPgMwGJmrH5TwMC#_:c14n0",
    "name": "Joel"
  }
}
```
