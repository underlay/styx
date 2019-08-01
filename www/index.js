let QUADS = null

const json = new JSONEditor(
	document.getElementById("json"),
	{
		mode: "view",
		modes: [],
		navigationBar: false,
		statusBar: false,
		search: false,
	},
	null
)

json.collapseAll()

const area = document.getElementById("blocklyArea")
const div = document.getElementById("blocklyDiv")
const workspace = Blockly.inject(div, {
	toolbox: document.getElementById("toolbox"),
})

const resize = _ => {
	const { offsetWidth, offsetHeight } = area

	let [element, x, y] = [area, 0, 0]

	do {
		x += element.offsetLeft
		y += element.offsetTop
		element = element.offsetParent
	} while (element)

	div.style.left = x + "px"
	div.style.top = y + "px"
	div.style.width = offsetWidth + "px"
	div.style.height = offsetHeight + "px"
	Blockly.svgResize(workspace)
}

window.addEventListener("resize", resize, false)
resize()

Blockly.svgResize(workspace)

const context = {
	schema: "http://schema.org/",
	prov: "http://www.w3.org/ns/prov#",
	rdfs: "http://www.w3.org/2000/01/rdf-schema#",
	rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	xsd: "http://www.w3.org/2001/XMLSchema#",
	u: "http://underlay.mit.edu/ns#",
}

const frame = {
	"@context": {
		...context,
		"prov:value": { "@container": "@list" },
	},
	"@requireAll": false,
	"u:satisfies": {},
	"prov:value": {},
	"prov:wasDerivedFrom": {},
}

const result = document.getElementById("result")
const query = document.getElementById("query")
query.addEventListener("click", async () => {
	if (QUADS === null) return

	const res = await fetch("/", {
		method: "POST",
		headers: {
			"Content-Type": "application/n-quads",
		},
		body: QUADS,
	})

	if (res.status === 200) {
		const r = await res.json()
		const {
			"@graph": [
				{ "u:satisfies": q, "prov:value": value, "prov:wasDerivedFrom": prov },
			],
		} = await jsonld.frame(r, frame)
		render(q, value, prov)
	} else {
		console.error(res.statusText)
	}
})

function render({ "@id": q }, value, prov) {
	const index = q.indexOf("#")
	result.innerHTML = `<tr><td>Query:</td><td class="query">&lt;${q}&gt;</td></tr>`
	if (Array.isArray(value) && value.length > 0 && prov != null) {
		for (const { "@id": id, "rdf:value": v } of value) {
			const tr = document.createElement("tr")
			const td1 = document.createElement("td")
			td1.innerText = id.slice(index + 1)
			const td2 = document.createElement("td")
			td2.innerHTML = renderValue(v)
			tr.appendChild(td1)
			tr.appendChild(td2)
			result.appendChild(tr)
		}
	} else {
		const tr = document.createElement("tr")
		const td = document.createElement("td")
		td.setAttribute("colspan", 2)
		td.innerText = "No results"
		tr.appendChild(td)
		result.appendChild(tr)
	}
}

function renderValue(value) {
	if (typeof value === "object") {
		if (value.hasOwnProperty("@id")) {
			return `&lt;${value["@id"]}&gt;`
		} else if (value.hasOwnProperty("@value")) {
			return JSON.stringify(value["@value"])
		}
	} else {
		return JSON.stringify(value)
	}
}

function walk(block, graph, label) {
	const children = block.getChildren()
	const id = block.getFieldValue("id")
	const subject = block.type === iri ? `<${expand(id)}>` : `_:${id}`
	if (children.length === 1) {
		const properties = []
		collect(children[0], properties)
		for (const property of properties) {
			if (property !== null) {
				const [id, value] = property
				const object = getValue(value, graph, label)
				graph.push(`${subject} <${expand(id)}> ${object} ${label}.`)
			}
		}
	}
}

function expand(id) {
	for (const key in context) {
		if (id.indexOf(key) === 0 && id[key.length] === ":") {
			return context[key] + id.slice(key.length + 1)
		}
	}
	return id
}

function collect(property, properties) {
	const children = property.getChildren()
	const id = property.getFieldValue("id")
	if (children.length === 0) {
		properties.push(null)
	} else if (children.length === 1) {
		if (children[0].type === predicate) {
			properties.push(null)
			collect(children[0], properties)
		} else {
			properties.push([id, children[0]])
		}
	} else if (children.length === 2) {
		properties.push([id, children[0]])
		collect(children[1], properties)
	}
}

function getValue(block, graph, label) {
	if (block.type === "math_number") {
		const n = Number(block.inputList[0].fieldRow[0].getValue())
		if (n === parseInt(n)) {
			return `"${n}"^^<${expand("xsd:integer")}>`
		} else {
			return `"${n}"^^<${expand("xsd:double")}>`
		}
	} else if (block.type === "text") {
		const s = block.inputList[0].fieldRow[1].getValue()
		return `"${escape(s)}"`
	} else if (block.type === "logic_boolean") {
		const b = block.inputList[0].fieldRow[0].getValue() === "TRUE"
		return `"${b}"^^<${expand("xsd:boolean")}>`
	} else if (block.type === variable || block.type === iri) {
		walk(block, graph, label)
		const id = block.getFieldValue("id")
		return block.type === iri ? `<${expand(id)}>` : `_:${id}`
	}
}

function escape(s) {
	return s
		.replace(/[\\"]/g, "\\$&")
		.replace(/\n/g, "\\n")
		.replace(/\r/g, "\\r")
		.replace(/\t/g, "\\t")
}

const clear = () => {
	QUADS = null
	query.setAttribute("disabled", true)
	json.update(null)
}

workspace.addChangeListener(() => {
	const topBlocks = workspace.getTopBlocks(true)
	const graph = []
	const label = "_:q"
	for (const block of topBlocks) {
		if (!block.rendered) continue
		if (block.type === variable || block.type === iri) {
			walk(block, graph, label)
		}
	}

	if (graph.length === 0) return clear()

	graph.push(`${label} <${expand("rdf:type")}> <${expand("u:Query")}> .\n`)
	const quads = graph.join("\n")
	jsonld
		.fromRDF(quads, { format: "application/n-quads", useNativeTypes: true })
		.then(doc => jsonld.compact(doc, context))
		.then(doc => {
			QUADS = quads
			query.removeAttribute("disabled")
			json.update(doc)
		})
		.catch(err => {
			console.error(err)
			clear()
		})
})
