import jsonld from "jsonld"

const baseOptions = {
	method: "POST",
	headers: { "Content-Type": "application/ld+json" },
}

const context = {
	dcterms: "http://purl.org/dc/terms/",
	prov: "http://www.w3.org/ns/prov#",
	rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	u: "http://underlay.mit.edu/ns#",
	"u:index": { "@container": "@list" },
}

const initialBody = extent => ({
	"@context": context,
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Bundle",
		"dcterms:extent": extent,
		"u:index": [],
		"u:enumerates": {
			"@graph": { "@type": "u:Graph" },
		},
	},
})

const nextBody = (extent, index) => ({
	"@context": context,
	"@type": "u:Query",
	"@graph": {
		"@type": "prov:Bundle",
		"dcterms:extent": extent,
		"u:index": {
			"@id": "_:b0",
			"rdf:value": { "@id": index },
		},
		"u:enumerates": {
			"@graph": {
				"@id": "_:b0",
				"@type": "u:Graph",
			},
		},
	},
})

export default async function listGraphs(extent, index) {
	const body = index ? nextBody(extent, index) : initialBody(extent)
	const options = { ...baseOptions, body: JSON.stringify(body) }
	const res = await fetch("/", options)
	const doc = await res.json()
	const [
		{
			"@graph": [
				{
					"http://www.w3.org/ns/prov#value": [{ "@list": values }],
				},
			],
			"http://underlay.mit.edu/ns#instanceOf": [{ "@id": q }],
		},
	] = await jsonld.expand(doc)

	const urls = []
	for (const {
		"http://www.w3.org/ns/prov#value": [
			{
				"@list": [value],
			},
		],
	} of values) {
		if (value === undefined) {
			urls.push(null)
		} else {
			urls.push(value["@id"])
		}
	}

	return urls
}
