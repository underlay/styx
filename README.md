# styx

Home-grown quadstore inspired by [Hexastore](https://dl.acm.org/citation.cfm?id=1453965), tailored for use in the Underlay.

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

```json
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
