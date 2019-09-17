import React from "react"
import ReactDOM from "react-dom"
import jsonld from "jsonld"

import * as Blockly from "blockly/core"

import { literal, iri, blank, blankPredicate, predicate } from "./blocks"

const right = document.getElementById("right")

let VARIABLE = 0

const fields = [
	'block[type="node"] block[type="blank"]',
	'block[type="blank-predicate"]',
	'block[type="blank"]',
].map(s => document.querySelector(`#toolbox > ${s} field`))

const QUERY = "http://underlay.mit.edu/ns#Query"
const ENTITY = "http://www.w3.org/ns/prov#Entity"
const RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

const SATISFIES = "http://underlay.mit.edu/ns#satisfies"

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

// const contextElement = document.getElementById("context")
// const contextEditor = new JSONEditor(contextElement, options, context)
// contextEditor.collapseAll()

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

addEventListener("resize", resize, false)
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

workspace.createVariable("b0", null, "0")
workspace.registerButtonCallback("variable", _ => {
	const name = `b${++VARIABLE}`
	const variable = workspace.createVariable(name)
	const id = variable.getId()
	for (const field of fields) {
		field.textContent = name
	}
})

class Query extends React.Component {
	constructor(props) {
		super(props)
		this.state = {
			disabled: true,
			quads: null,
			q: null,
			values: null,
			prov: null,
		}
	}

	componentDidMount() {
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

			if (quads.length === 0) {
				this.setState({ disabled: true, quads: null })
			} else {
				// quads.push(`${g} <${RDF_TYPE}> <${QUERY}> .\n`)
				quads.push(`_:e <${SATISFIES}> ${g} _:p .`)
				quads.push(`_:e <${RDF_TYPE}> <${ENTITY}> _:p .`)
				quads.push(`_:p <${RDF_TYPE}> <${QUERY}> .\n`)

				this.setState({ quads: quads.join("\n"), disabled: false })
			}
		})
	}

	handleClick = async () => {
		if (this.state.quads === null) {
			return
		}

		console.log(this.state.quads)

		const res = await fetch("/", {
			method: "POST",
			headers: {
				"Content-Type": "application/n-quads",
			},
			body: this.state.quads,
		})

		if (res.status === 200) {
			const r = await res.json()
			console.log(r)
			const {
				"@graph": [
					{
						"u:satisfies": q,
						"prov:value": values,
						"prov:wasDerivedFrom": prov,
					},
				],
			} = await jsonld.frame(r, frame)
			this.setState({ q, values, prov })
		} else {
			console.error(res.statusText)
			this.setState({ q: null, values: null, prov: null })
		}
	}

	render() {
		const { disabled, q, values, prov } = this.state
		return (
			<React.Fragment>
				<button disabled={disabled} id="query" onClick={this.handleClick}>
					Query
				</button>
				<details>
					<summary>Context</summary>
					Something small enough to escape casual notice.
				</details>
				<div id="context"></div>
				{Query.renderResult(q, values, prov)}
			</React.Fragment>
		)
	}

	static renderResult(q, values, prov) {
		if (q === null || values === null) {
			return null
		}

		const index = q["@id"].indexOf("#")

		return (
			<table>
				<tbody>
					<tr>
						<td colSpan="2" className="query">
							&lt;{q["@id"]}&gt;
						</td>
					</tr>
					{Array.isArray(values) && values.length > 0 && prov !== null ? (
						values.map(({ "@id": id, "rdf:value": v }, key) => (
							<tr key={key}>
								<td>{id.slice(index + 1)}</td>
								<td>{Query.renderValue(v)}</td>
							</tr>
						))
					) : (
						<td>
							<td colSpan="2">No results</td>
						</td>
					)}
				</tbody>
			</table>
		)
	}

	static renderValue(value) {
		if (typeof value === "object") {
			if (value.hasOwnProperty("@id")) {
				return `<${value["@id"]}>`
			} else if (value.hasOwnProperty("@value")) {
				return JSON.stringify(value["@value"])
			}
		} else {
			return JSON.stringify(value)
		}
	}
}

ReactDOM.render(<Query />, right)
