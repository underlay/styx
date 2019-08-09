let QUADS = null
let VARIABLE = 0

const fields = [
	'block[type="node"] block[type="blank"]',
	'block[type="blank-predicate"]',
	'block[type="blank"]',
].map(s => document.querySelector(`#toolbox > ${s} field`))

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
	console.log(QUADS)
	const res = await fetch("/", {
		method: "POST",
		headers: {
			"Content-Type": "application/n-quads",
		},
		body: QUADS,
	})

	if (res.status === 200) {
		const r = await res.json()
		console.log(r)
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

function walk(block, quads, nodes, g) {
	let value, type
	switch (block.type) {
		case "text":
			value = escape(block.getFieldValue("TEXT"))
			return `"${value}"`
		case "math_number":
			value = Number(block.getFieldValue("NUM"))
			type = value === parseInt(value) ? XSD_INTEGER : XSD_DOUBLE
			return `"${value}"^^<${type}>`
		case "logic_boolean":
			value = block.getFieldValue("BOOL").toLowerCase()
			return `"${value}"^^<${XSD_BOOLEAN}>`
		case literal:
			value = block.getFieldValue("value")
			type = block.getFieldValue("type")
			return `"${escape(value)}"^^<${expand(type)}>`
		case iri:
			return `<${expand(block.getFieldValue("id"))}>`
		case blank:
			return `_:${workspace.getVariableById(block.getFieldValue("id")).name}`
		case blankPredicate:
		case predicate:
			return
	}

	const subject = block.getInputTargetBlock("subject")
	if (subject === null) {
		return null
	}

	const s = walk(subject, quads, nodes, g)

	for (
		let property = block.getInputTargetBlock("predicate");
		property !== null;
		property = property.getNextBlock()
	) {
		const id = property.getFieldValue("id")
		const p =
			property.type === blankPredicate
				? `_:${workspace.getVariableById(id).name}`
				: `<${expand(id)}>`
		const object = property.getInputTargetBlock("object")
		if (object !== null) {
			const o = walk(object, quads, nodes, g)
			if (o !== null) {
				quads.push(`${s} ${p} ${o} ${g} .`)
			}
		}
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
}

workspace.createVariable("b0", null, "0")
workspace.registerButtonCallback("variable", _ => {
	const name = `b${++VARIABLE}`
	const variable = workspace.createVariable(name)
	const id = variable.getId()
	for (const field of fields) {
		field.textContent = name
	}
})

workspace.addChangeListener(() => {
	const nodes = []
	const quads = []
	const g = "_:q"

	for (const block of workspace.getTopBlocks(true)) {
		if (block.rendered) {
			walk(block, quads, nodes, g)
		}
	}

	workspace.updateToolbox(toolbox)

	if (quads.length === 0) return clear()
	quads.push(`${g} <${RDF_TYPE}> <${QUERY_TYPE}> .\n`)
	QUADS = quads.join("\n")
	query.removeAttribute("disabled")
})
