import React from "react"
import ReactDOM from "react-dom"
import jsonld from "jsonld"

import Message from "explore"

import { base58 } from "explore/src/utils.js"

const main = document.querySelector("main")

const title = document.title
const url = location.origin + location.pathname

const options = {
	method: "POST",
	headers: { "Content-Type": "application/ld+json" },
	body: JSON.stringify({
		"@context": {
			dcterms: "http://purl.org/dc/terms/",
			prov: "http://www.w3.org/ns/prov#",
			rdfs: "http://www.w3.org/2000/01/rdf-schema#",
			rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
			xsd: "http://www.w3.org/2001/XMLSchema#",
			u: "http://underlay.mit.edu/ns#",
		},
		"@type": "u:Query",
		"@graph": {
			"@type": "prov:Bundle",
			"dcterms:extent": 5,
			"u:enumerates": {
				"@graph": {
					"@type": "u:Graph",
				},
			},
		},
	}),
}

class Browse extends React.Component {
	constructor(props) {
		super(props)
		const match = base58.exec(location.search.slice(1))
		if (match === null) {
			this.state = { messages: null, cid: null, focus: null }
		} else if (location.hash === "" && location.href.slice(-1) === "#") {
			this.state = { messages: null, cid: match[0], focus: "" }
		} else if (location.hash === "") {
			this.state = { messages: null, cid: match[0], focus: null }
		} else {
			this.state = {
				messages: null,
				cid: match[0],
				focus: location.hash.slice(1),
			}
		}

		history.replaceState(
			{ cid: this.state.cid, focus: this.state.focus },
			title,
			url +
				(this.state.cid === null ? "" : "?" + this.state.cid) +
				(this.state.focus === null ? "" : "#" + this.state.focus)
		)
	}

	componentDidMount() {
		addEventListener("hashchange", () => {
			const state = {}
			if (location.hash === "" && location.href.slice(-1) === "#") {
				state.focus = ""
			} else if (location.hash === "") {
				state.focus = null
			} else {
				state.focus = location.hash.slice(1)
			}

			history.replaceState(
				{ cid: this.state.cid, focus: state.focus },
				title,
				url + location.search + (state.focus === null ? "" : "#" + state.focus)
			)

			this.setState(state)
		})

		if (this.state.cid === null) {
			this.fetchMessages()
		}
	}

	componentDidUpdate(prevProps, prevState) {
		if (this.state.cid === null && this.state.messages === null) {
			this.fetchMessages()
		}
	}

	async fetchMessages() {
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
				break
			}
			const { "@id": id } = value
			urls.push(new URL(id))
		}

		const messages = []

		urls.reduce((previous, { pathname, hash }, i) => {
			const cid = pathname.split("/").pop()
			if (previous !== null && cid === previous.cid) {
				previous.graphs.push(hash.slice(1))
				return previous
			} else {
				const message = { cid, graphs: [hash.slice(1)] }
				messages.push(message)
				return message
			}
		}, null)

		this.setState({ messages })
	}

	handleFocus = focus => {
		if (focus === null) {
			history.pushState(
				{ cid: this.state.cid, focus },
				title,
				url + location.search
			)
			this.setState({ focus })
		} else {
			location.hash = focus
		}
	}

	render() {
		const { messages, cid, focus } = this.state
		if (cid !== null) {
			return (
				<Message
					path={"/ipfs/" + cid}
					focus={focus}
					onFocus={this.handleFocus}
				/>
			)
		} else if (messages === null) {
			return <p>Loading...</p>
		} else if (messages.length === 0) {
			return <p>No graphs found</p>
		} else {
			return <React.Fragment>{messages.map(this.renderMessage)}</React.Fragment>
		}
	}

	renderMessage = ({ cid, graphs }) => (
		<fieldset key={cid}>
			<legend>
				<span>{cid}</span> ·
			</legend>
			<a href={`?${cid}`}>View</a>
			<br />
			<a href={`http://localhost:8080/ipfs/${cid}`}>Download</a>
		</fieldset>
	)
}

ReactDOM.render(<Browse />, main)
