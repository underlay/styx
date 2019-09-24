import jsonld from "jsonld"

const iri = "(?:<([^:]+:[^>]*)>)"
const plain = '"([^"\\\\]*(?:\\\\.[^"\\\\]*)*)"'
const datatype = `(?:\\^\\^${iri})`
const language = "(?:@([a-z]+(?:-[a-zA-Z0-9]+)*))"
const literal = `(?:${plain}(?:${datatype}|${language})?)`
export const object = new RegExp(`^(?:${iri}|${literal})$`)

export const baseOptions = {
	method: "POST",
	headers: { "Content-Type": "application/ld+json" },
}

const context = {
	dcterms: "http://purl.org/dc/terms/",
	prov: "http://www.w3.org/ns/prov#",
	rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	u: "http://underlay.mit.edu/ns#",
}

function makeQuery(extent, index, enumerator) {
	const domain = { "@id": predicateId }
	if (index && index !== null) {
		domain["u:index"] = { "@id": index }
	}
	return {
		"@type": "u:Query",
		"@graph": {
			"@type": "prov:Bundle",
			"dcterms:extent": extent,
			"u:domain": domain,
			"u:enumerates": { "@graph": enumerator },
		},
	}
}

function makeLeftPredicate(quads, term, extent, index) {
	quads.push(
		"_:l <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://underlay.mit.edu/ns#Query> .",
		"_:l-bundle <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Bundle> _:l .",
		`_:l-bundle <http://purl.org/dc/terms/extent> "${extent}"^^<http://www.w3.org/2001/XMLSchema#integer> _:l .`,
		`_:l-bundle <http://underlay.mit.edu/ns#domain> _:predicate _:l .`,
		"_:l-bundle <http://underlay.mit.edu/ns#enumerates> _:l-enumerator _:l .",
		`_:subject _:predicate ${term} _:l-enumerator .`
	)

	if (index !== null) {
		quads.push(`_:predicate <http://underlay.mit.edu/ns#index> ${index} _:l .`)
	}
}

function makeRightPredicate(quads, term, extent, index) {
	quads.push(
		"_:r <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://underlay.mit.edu/ns#Query> .",
		"_:r-bundle <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Bundle> _:r .",
		`_:r-bundle <http://purl.org/dc/terms/extent> "${extent}"^^<http://www.w3.org/2001/XMLSchema#integer> _:r .`,
		`_:r-bundle <http://underlay.mit.edu/ns#domain> _:predicate _:r .`,
		"_:r-bundle <http://underlay.mit.edu/ns#enumerates> _:r-enumerator _:r .",
		`${term} _:predicate _:object _:r-enumerator .`
	)

	if (index !== null) {
		quads.push(`_:predicate <http://underlay.mit.edu/ns#index> ${index} _:r .`)
	}
}

export async function predicateQuery(term, extent, left, right) {
	const [_, uri, value, datatype, language] = object.exec(term)

	const quads = []
	makeLeftPredicate(quads, term, extent, left)
	if (uri !== undefined) {
		makeRightPredicate(quads, term, extent, right)
	}
	quads.push("")

	const res = await fetch("/", {
		method: "POST",
		headers: { "Content-Type": "application/n-quads" },
		body: quads.join("\n"),
	})

	const doc = await res.json()

	const predicates = {}
	for (const {
		"@graph": [
			{
				"http://www.w3.org/ns/prov#value": [{ "@list": values }],
			},
		],
		"http://underlay.mit.edu/ns#instanceOf": [{ "@id": q }],
	} of await jsonld.expand(doc)) {
		const parity = q.slice(q.indexOf("#"))
		predicates[parity] = []

		for (const {
			"http://www.w3.org/ns/prov#value": [{ "@list": assignments }],
		} of values) {
			if (assignments.length === 0) {
				break
			}

			const {
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#value": [
					{ "@id": predicate },
				],
			} = assignments.find(
				({ "@id": id }) => id.slice(id.indexOf("#")) === "#_:predicate"
			)

			const {
				"http://www.w3.org/1999/02/22-rdf-syntax-ns#value": [value],
			} = assignments.find(
				({ "@id": id }) => id.slice(id.indexOf("#")) !== "#_:predicate"
			)

			predicates[parity].push([predicate, value])
		}
	}

	return [predicates["#_:l"] || [], predicates["#_:r"] || []]
}

function makeLeftLeaf(quads, graph, term, predicate, extent, index) {
	quads.push(
		`_:${graph} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://underlay.mit.edu/ns#Query> .`,
		`_:${graph}-bundle <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Bundle> _:${graph} .`,
		`_:${graph}-bundle <http://purl.org/dc/terms/extent> "${extent}"^^<http://www.w3.org/2001/XMLSchema#integer> _:${graph} .`,
		`_:${graph}-bundle <http://underlay.mit.edu/ns#domain> _:${graph}-value _:${graph} .`,
		`_:${graph}-bundle <http://underlay.mit.edu/ns#enumerates> _:${graph}-enumerator _:${graph} .`,
		`_:${graph}-value <${predicate}> ${term} _:${graph}-enumerator .`
	)

	if (index !== null) {
		quads.push(
			`_:${graph}-value <http://underlay.mit.edu/ns#index> ${index} _:${graph} .`
		)
	}
}

function makeRightLeaf(quads, graph, term, predicate, extent, index) {
	quads.push(
		`_:${graph} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://underlay.mit.edu/ns#Query> .`,
		`_:${graph}-bundle <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Bundle> _:${graph} .`,
		`_:${graph}-bundle <http://purl.org/dc/terms/extent> "${extent}"^^<http://www.w3.org/2001/XMLSchema#integer> _:${graph} .`,
		`_:${graph}-bundle <http://underlay.mit.edu/ns#domain> _:${graph}-value _:${graph} .`,
		`_:${graph}-bundle <http://underlay.mit.edu/ns#enumerates> _:${graph}-enumerator _:${graph} .`,
		`${term} <${predicate}> _:${graph}-value _:${graph}-enumerator .`
	)

	if (index !== null) {
		quads.push(
			`_:${graph}-value <http://underlay.mit.edu/ns#index> ${index} _:${graph} .`
		)
	}
}

export async function leafQuery(
	id,
	[leftPredicates, rightPredicates],
	extent,
	left,
	right
) {
	const [_, uri, value, datatype, language] = object.exec(id)

	const quads = []

	for (let i = 0; i < leftPredicates.length; i++) {
		const [predicate, value] = leftPredicates[i]
		makeLeftLeaf(quads, `l${i}`, id, predicate, extent, left)
	}

	if (uri !== undefined) {
		for (let i = 0; i < rightPredicates.length; i++) {
			const [predicate, value] = rightPredicates[i]
			makeRightLeaf(quads, `r${i}`, id, predicate, extent, right)
		}
	}

	quads.push("")

	const res = await fetch("/", {
		method: "POST",
		headers: { "Content-Type": "application/n-quads" },
		body: quads.join("\n"),
	})

	const doc = await res.json()

	const leaves = {}
	for (const {
		"@graph": [
			{
				"http://www.w3.org/ns/prov#value": [{ "@list": values }],
			},
		],
		"http://underlay.mit.edu/ns#instanceOf": [{ "@id": q }],
	} of await jsonld.expand(doc)) {
		const hash = q.slice(q.indexOf("#") + 1)
		leaves[hash] = []

		for (const {
			"http://www.w3.org/ns/prov#value": [{ "@list": assignments }],
		} of values) {
			if (assignments.length === 0) {
				break
			}

			const [
				{
					"http://www.w3.org/1999/02/22-rdf-syntax-ns#value": [value],
				},
			] = assignments

			leaves[hash].push(value)
		}
	}

	const leftLeaves = new Array({ length: leftPredicates.length })
	for (let i = 0; i < leftPredicates.length; i++) {
		leftLeaves[i] = leaves[`_:l${i}`]
	}

	const rightLeaves = new Array(rightPredicates.length)
	for (let i = 0; i < rightPredicates.length; i++) {
		rightLeaves[i] = leaves[`_:r${i}`]
	}

	return [leftLeaves, rightLeaves]
}
