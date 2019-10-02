import React from "react"
import ReactDOM from "react-dom"
import N3Store from "n3/lib/N3Store"
import { getInitialContext, process } from "jsonld/lib/context"
import { compactIri } from "jsonld/lib/compact"
import Graph from "explore/src/graph.jsx"
import Node from "explore/src/node.js"
import { RDF_TYPE, decode } from "explore/src/utils.js"

import localCtx from "explore/src/context.json"

import query from "./query.js"

const processingMode = "json-ld-1.1"

const main = document.querySelector("main")

const title = document.title
const url = location.origin + location.pathname

function getContext(base) {
	const activeCtx = getInitialContext({ base })
	return process({ activeCtx, localCtx, processingMode })
}

const ctx = getContext("")

const compact = (iri, vocab) =>
	compactIri({ activeCtx: ctx, iri, relativeTo: { vocab: !!vocab } })

const wrap = f => ({ target }) => f(decode(target.id()))

class Browse extends React.Component {
	static SubjectPageSize = 10
	static ObjectPageSize = 10
	static Examples = [
		"http://schema.org/DigitalDocument",
		"dweb:/ipfs/QmfCtdbfajVvzTsoUDMLBWvJtnh6mpA6SQTBwrMWCEhmdt",
	]

	static Null = {
		value: null,
		id: null,
		store: null,
	}

	static validateURI(text) {
		try {
			new URL(text)
		} catch (e) {
			return false
		}
		return true
	}

	constructor(props) {
		super(props)
		const id = decodeURIComponent(location.hash.slice(1))
		if (Browse.validateURI(id)) {
			this.state = { ...Browse.Null, id }
		} else {
			this.state = { ...Browse.Null, value: "" }
		}

		this.cy = null

		const hash = this.state.id === null ? "" : location.hash

		history.replaceState({ id }, title, url + hash)
	}

	componentDidMount() {
		addEventListener("hashchange", () => {
			const id = decodeURIComponent(location.hash.slice(1))
			if (Browse.validateURI(id) && id !== this.state.id) {
				history.replaceState({ id }, title, url + location.hash)
				if (this.state.store === null) {
					const store = new N3Store()
					window.store = store
					query(
						id,
						Browse.SubjectPageSize,
						[],
						Browse.ObjectPageSize,
						[],
						store
					).then(() => this.setState({ id, store }))
				} else {
					this.state.store.forEach(quad => {
						if (quad.subject.id !== id && quad.object.id !== id) {
							store.removeQuad(quad)
						}
					})

					query(
						id,
						Browse.SubjectPageSize,
						[],
						Browse.ObjectPageSize,
						[],
						this.state.store
					).then(() => this.setState({ id }))
				}
			} else {
				history.replaceState({ id: this.state.id }, title, url)
				this.setState({ ...Browse.Null, value: this.state.value || "" })
			}
		})

		if (this.state.id !== null) {
			const store = new N3Store()
			window.store = store
			query(
				this.state.id,
				Browse.SubjectPageSize,
				[],
				Browse.ObjectPageSize,
				[],
				store
			).then(() => this.setState({ store }))
		}
	}

	componentDidUpdate(prevProps, prevState, snapshot) {
		if (prevState.id === this.state.id || this.state.store === null) {
			return
		}

		const nodes = {}
		const edgeData = {}
		const nodeData = {}
		const quads = this.state.store.getQuads()

		for (const { subject, predicate, object, graph } of quads) {
			Graph.createNode(subject, nodes, null)

			const iri = predicate.id

			if (object.termType === "Literal") {
				const { literals } = nodes[subject.id]
				if (Array.isArray(literals[iri])) {
					literals[iri].push(object)
				} else {
					literals[iri] = [object]
				}
			} else if (object.termType === "NamedNode" && iri === RDF_TYPE) {
				nodes[subject.id].types.push(object.id)
			} else {
				Graph.createNode(object, nodes, null)

				const id = encode(graph.id)
				const name = compact(iri, true)
				const [source, target] = [subject.id, object.id].map(encode)
				edgeData[graph.id] = { id, iri, name, source, target }
			}
		}

		for (const id in nodes) {
			const { literals, types } = nodes[id]
			const [svg, width, height] = Node(id, types, literals, compact)
			nodeData[id] = {
				id: encode(id),
				svg: Graph.DataURIPrefix + encodeURIComponent(Graph.SVGPrefix + svg),
				width,
				height,
			}
		}

		this.cy.batch(() => {
			this.cy.nodes().forEach(ele => {
				if (ele.removed()) {
					return
				}
				const id = decode(ele.id())
				if (nodeData.hasOwnProperty(id)) {
					const { svg, width, height } = nodeData[id]
					ele.data({ svg, width, height })
					delete nodeData[id]
				} else {
					ele.remove()
				}
			})

			this.cy
				.add(
					Object.keys(nodeData).map(id => ({
						group: "nodes",
						data: nodeData[id],
					}))
				)
				.on("mouseover", wrap(this.handleMouseOver))
				.on("mouseout", wrap(this.handleMouseOut))
				.on("select", wrap(this.handleSelect))
				.on("unselect", wrap(this.handleUnselect))

			this.cy.edges().forEach(ele => {
				const id = decode(ele.id())
				if (edgeData.hasOwnProperty(id)) {
					delete edgeData[id]
				} else {
					ele.remove()
				}
			})

			for (const id in edgeData) {
				this.cy.add({ group: "edges", data: edgeData[id] })
			}

			this.cy
				.layout({
					name: "breadthfirst",
					roots: `#${encode(this.state.id)}`,
					circle: false,
					spacingFactor: 1,
					animate: true,
					padding: 50,
				})
				.run()
				.on("layoutstop", () => this.cy.animate({ fit: { padding: 50 } }))
		})
	}

	handleSubmit = event => {
		event.preventDefault()
		if (Browse.validateURI(this.state.value)) {
			location.hash = encodeURIComponent(this.state.value)
		}
	}

	handleChange = ({ target: { value } }) => this.setState({ value })
	handleSelect = focus => (location.hash = encodeURIComponent(focus))
	handleUnselect = id => {}
	handleMouseOver = id => {}
	handleMouseOut = id => {}
	handleMount = cy => (this.cy = window.cy = cy)
	handleDestroy = () => {}

	render() {
		const { id, value } = this.state
		if (id === null) {
			const disabled = !Browse.validateURI(value)
			return (
				<div className="empty">
					<p>Enter a URI:</p>
					<form onSubmit={this.handleSubmit}>
						<input
							type="text"
							placeholder="http://..."
							value={value}
							onChange={this.handleChange}
						/>
						<input type="submit" disabled={disabled} value="Go" />
					</form>
					<p>Examples:</p>
					<ul>
						{Browse.Examples.map(term => (
							<li key={term}>
								<a href={`#${encodeURIComponent(term)}`}>{term}</a>
							</li>
						))}
					</ul>
				</div>
			)
		} else {
			return (
				<React.Fragment>
					<section>
						<div className="path"></div>
						<button>Page more incoming properties</button> |{" "}
						<button>Page more outgoing properties</button>
					</section>
					{this.renderGraph()}
				</React.Fragment>
			)
		}
	}
	renderGraph() {
		const { store, id } = this.state
		if (store === null) {
			return <p>Loading...</p>
		} else if (store.size === 0) {
			return <p>No data found for {id}</p>
		} else {
			return (
				<Graph
					focus={id}
					graph={null}
					store={store}
					context={ctx}
					onSelect={this.handleSelect}
					onUnselect={this.handleUnselect}
					onMouseOver={this.handleMouseOver}
					onMouseOut={this.handleMouseOut}
					onMount={this.handleMount}
					onDestroy={this.handleDestroy}
				/>
			)
		}
	}
}

ReactDOM.render(<Browse />, main)
