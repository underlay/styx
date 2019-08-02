let QUADS = null
let BLANK_ID = 0
const blankTest = /^b(0|[1-9]\d*)$/
const blankField = document.getElementById("blank-id")

const QUERY_TYPE = "http://underlay.mit.edu/ns#Query"
const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

const XSD_STRING = "http://www.w3.org/2001/XMLSchema#string"
const XSD_BOOLEAN = "http://www.w3.org/2001/XMLSchema#boolean"
const XSD_INTEGER = "http://www.w3.org/2001/XMLSchema#integer"
const XSD_DOUBLE = "http://www.w3.org/2001/XMLSchema#double"
const XSD_DATE = "http://www.w3.org/2001/XMLSchema#date"
const XSD_DATETIME = "http://www.w3.org/2001/XMLSchema#dateTime"
const FONT_SIZE = 12

const TAB = 2
const CHAR = 7.2
const LINE_HEIGHT = 18
const FONT_FAMILY = "Monaco, monospace"

const context = {
	schema: "http://schema.org/",
	prov: "http://www.w3.org/ns/prov#",
	rdfs: "http://www.w3.org/2000/01/rdf-schema#",
	rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	xsd: "http://www.w3.org/2001/XMLSchema#",
	nt: "http://semanticbible.org/ns/2006/NTNames#",
	u: "http://underlay.mit.edu/ns#",
}

const options = {
	mode: "view",
	modes: [],
	navigationBar: false,
	statusBar: false,
	search: false,
}

const contextElement = document.getElementById("context")
const contextEditor = new JSONEditor(contextElement, options, context)
contextEditor.collapseAll()

const jsonElement = document.getElementById("json")
const jsonEditor = new JSONEditor(jsonElement, options, {})
jsonEditor.collapseAll()

const toolbox = document.getElementById("toolbox")
const area = document.getElementById("blocklyArea")
const div = document.getElementById("blocklyDiv")
const workspace = Blockly.inject(div, { toolbox })

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
	result.innerHTML = `<tr><td colspan="2" class="query">&lt;${q}&gt;</td></tr>`
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

function walk(block, graph, g) {
	switch (block.type) {
		case iri:
			return `<${expand(block.getFieldValue("id"))}>`
		case blank:
			const l = block.getFieldValue("id")
			const m = blankTest.exec(l)
			if (m !== null) {
				BLANK_ID = Math.max(BLANK_ID, parseInt(m[1]) + 1)
			}
			return `_:${l}`
		case "text":
			const value = escape(block.inputList[0].fieldRow[1].getValue())
			return `"${value}"`
		case "math_number":
			const n = Number(block.inputList[0].fieldRow[0].getValue())
			const type = n === parseInt(n) ? XSD_INTEGER : XSD_DOUBLE
			return `"${n}"^^<${type}>`
		case "logic_boolean":
			const b = block.inputList[0].fieldRow[0].getValue() === "TRUE"
			return `"${b}"^^<${XSD_BOOLEAN}>`
	}

	const subject = block.getInputTargetBlock("subject")
	if (subject === null) {
		return null
	}

	const s = walk(subject, graph, g)

	let predicate = block.getInputTargetBlock("predicate")
	while (predicate !== null) {
		const id = predicate.getFieldValue("id")
		const object = predicate.getInputTargetBlock("object")
		if (object !== null) {
			const o = walk(object, graph, g)
			if (o !== null) {
				graph.push(`${s} <${expand(id)}> ${o} ${g}.`)
			}
		}

		predicate = predicate.getNextBlock()
	}

	return s
}

function expand(id) {
	for (const key in context) {
		if (id.indexOf(key) === 0 && id[key.length] === ":") {
			return context[key] + id.slice(key.length + 1)
		}
	}
	return id
}

const escape = s =>
	s
		.replace(/[\\"]/g, "\\$&")
		.replace(/\n/g, "\\n")
		.replace(/\r/g, "\\r")
		.replace(/\t/g, "\\t")

const clear = () => {
	QUADS = null
	query.setAttribute("disabled", true)
	jsonEditor.update({})
}

workspace.addChangeListener(() => {
	const graph = []
	const g = "_:q"

	BLANK_ID = 0
	for (const block of workspace.getTopBlocks(true)) {
		if (block.rendered) {
			walk(block, graph, g)
		}
	}

	blankField.innerText = `b${BLANK_ID}`
	workspace.updateToolbox(toolbox)

	if (graph.length === 0) return clear()

	graph.push(`${g} <${RDF_TYPE}> <${QUERY_TYPE}> .\n`)
	const quads = graph.join("\n")
	jsonld
		.fromRDF(quads, { format: "application/n-quads", useNativeTypes: true })
		.then(doc => jsonld.compact(doc, context))
		.then(doc => {
			QUADS = quads
			query.removeAttribute("disabled")
			jsonEditor.update(doc)
		})
		.catch(err => {
			console.error(err)
			clear()
		})
})
