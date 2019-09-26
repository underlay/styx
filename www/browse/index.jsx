import React from "react"
import ReactDOM from "react-dom"
import N3Store from "n3/lib/N3Store"
import { getInitialContext, process } from "jsonld/lib/context"
import Graph from "explore/src/graph"

window.N3Store = N3Store

import localCtx from "explore/src/context.json"

import query from "./query"

const processingMode = "json-ld-1.1"

const main = document.querySelector("main")

const title = document.title
const url = location.origin + location.pathname

function getContext(base) {
	const activeCtx = getInitialContext({ base })
	return process({ activeCtx, localCtx, processingMode })
}

const ctx = getContext("")

class Browse extends React.Component {
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

		const hash = this.state.id === null ? "" : location.hash

		history.replaceState({ id }, title, url + hash)
	}

	componentDidMount() {
		addEventListener("hashchange", () => {
			const id = decodeURIComponent(location.hash.slice(1))
			if (Browse.validateURI(id)) {
				this.setState({ ...Browse.Null, id }, () => this.fetch())
				history.replaceState({ id: this.state.id }, title, url + location.hash)
			} else {
				this.setState({ ...Browse.Null, value: this.state.value || "" })
				history.replaceState({ id: this.state.id }, title, url)
			}
		})

		if (this.state.id !== null) {
			this.fetch()
		}
	}

	async fetch() {
		const store = new N3Store()
		window.store = store
		await query(this.state.id, 2, [], 2, [], store)
		this.setState({ store })
	}

	handleSubmit = event => {
		event.preventDefault()
		if (Browse.validateURI(this.state.value)) {
			location.hash = `#${encodeURIComponent(this.state.value)}`
		}
	}

	handleChange = ({ target: { value } }) => this.setState({ value })

	handleSelect = id => {}
	handleUnselect = id => {}
	handleMouseOver = id => {}
	handleMouseOut = id => {}
	handleMount = cy => {}
	handleDestroy = cy => {}

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
