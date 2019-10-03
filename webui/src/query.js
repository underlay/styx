import jsonld from "jsonld"
window.jsonld = jsonld

const baseOptions = {
	method: "POST",
	headers: { "Content-Type": "application/n-quads" },
}

export const RDFValue = "http://www.w3.org/1999/02/22-rdf-syntax-ns#value"

function makeBundle(quads, q, extent, index, enumerator) {
	quads.push(
		`_:${q} <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://underlay.mit.edu/ns#Query> .`,
		`_:${q}-b <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Bundle> _:${q} .`,
		`_:${q}-b <http://purl.org/dc/terms/extent> "${extent}"^^<http://www.w3.org/2001/XMLSchema#integer> _:${q} .`
	)
	const head = index.reduceRight(
		(head, { "@id": id, [RDFValue]: value }, i) => {
			const next = `_:${q}-${i}`
			quads.push(
				`${next} <http://www.w3.org/1999/02/22-rdf-syntax-ns#first> <${id}> _:${q} .`,
				`${next} <http://www.w3.org/1999/02/22-rdf-syntax-ns#rest> ${head} _:${q} .`
			)
			if (value !== undefined) {
				quads.push(`<${id}> <${RDFValue}> ${parseValue(value)} _:${q} .`)
			}
			return next
		},
		"<http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>"
	)
	quads.push(
		`_:${q}-b <http://underlay.mit.edu/ns#index> ${head} _:${q} .`,
		`_:${q}-b <http://underlay.mit.edu/ns#enumerates> ${enumerator} _:${q} .`
	)
}

function parseValue(value) {
	if (typeof value === "string") {
		return JSON.stringify(value)
	} else if (typeof value === "number") {
		if (value === parseInt(value)) {
			return `"${value}"^^<http://www.w3.org/2001/XMLSchema#integer>`
		} else {
			return `"${value}"^^<http://www.w3.org/2001/XMLSchema#double>`
		}
	} else if (typeof value === "boolean") {
		return `"${value}"^^<http://www.w3.org/2001/XMLSchema#boolean>`
	} else if (typeof value === "object") {
		const { "@value": v, "@type": t, "@language": l } = value
		if (l !== undefined) {
			return `${JSON.stringify(v)}@${l}`
		} else if (t === "http://www.w3.org/2001/XMLSchema#string") {
			return JSON.stringify(v)
		} else if (t !== undefined) {
			return `${JSON.stringify(v)}^^<${t}>`
		} else {
			return JSON.stringify(v)
		}
	}
}

/**
 *
 * @param {string} id
 * @param {number} subjectExtent
 * @param {number} subjectIndex
 * @param {Array} objectExtent
 * @param {Array} objectIndex
 * @param {N3Store} store
 */
export default async function query(
	id,
	subjectExtent,
	subjectIndex,
	objectExtent,
	objectIndex,
	store
) {
	const quads = []
	makeBundle(quads, "s", subjectExtent, subjectIndex, "_:s-e")
	quads.push(`<${id}> _:predicate _:object _:s-e .`)
	makeBundle(quads, "o", objectExtent, objectIndex, "_:o-e")
	quads.push(`_:subject _:predicate <${id}> _:o-e .`)
	quads.push("")

	const res = await fetch("/", {
		...baseOptions,
		body: quads.join("\n"),
	})
	const doc = await res.json()
	const s = doc["@graph"].find(({ "u:instanceOf": id }) => id === "q:_:s")
	const o = doc["@graph"].find(({ "u:instanceOf": id }) => id === "q:_:o")

	for (const { value, wasDerivedFrom } of s["@graph"].value) {
		if (value === undefined || value.length === 0) {
			break
		}

		const {
			"rdf:value": { "@id": predicate },
		} = value.find(({ "@id": id }) => id === "q:_:predicate")

		const { "rdf:value": object } = value.find(
			({ "@id": id }) => id === "q:_:object"
		)

		const {
			hadMember: [{ "@id": graph }],
		} = wasDerivedFrom

		if (!(graph in store._graphs)) {
			if (typeof object === "object" && typeof object["@id"] === "string") {
				store.addQuad(id, predicate, object["@id"], graph)
			} else {
				store.addQuad(id, predicate, parseValue(object), graph)
			}
		}
	}

	for (const { value, wasDerivedFrom } of o["@graph"].value) {
		if (value === undefined || value.length === 0) {
			break
		}

		const {
			"rdf:value": { "@id": subject },
		} = value.find(({ "@id": id }) => id === "q:_:subject")

		const {
			"rdf:value": { "@id": predicate },
		} = value.find(({ "@id": id }) => id === "q:_:predicate")

		const {
			hadMember: [{ "@id": graph }],
		} = wasDerivedFrom

		if (!(graph in store._graphs)) {
			store.addQuad(subject, predicate, id, graph)
		}
	}
}
