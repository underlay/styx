import React from "react"
import ReactDOM from "react-dom"

import Message from "explore"

import { base58 } from "explore/src/utils.js"
import listGraphs from "./fetch"

const main = document.querySelector("main")

const title = document.title
const url = location.origin + location.pathname

const graphURI = /^ul:\/ipfs\/[123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]{46}#(_:[a-zA-Z0-9-]+)?$/

class Browse extends React.Component {
	static Gateway = "http://localhost:8080"
	static PageSize = 10
	static Null = { index: null, graphs: null, cid: null, focus: null }
	constructor(props) {
		super(props)
		const match = base58.exec(location.search.slice(1))
		if (match === null) {
			let index = null
			if (location.search.indexOf("?index=") === 0) {
				const uri = decodeURIComponent(location.search.slice(7))
				if (graphURI.test(uri)) {
					index = uri
				}
			}
			this.state = { ...Browse.Null, index }
		} else if (location.hash === "" && location.href.slice(-1) === "#") {
			this.state = { ...Browse.Null, cid: match[0], focus: "" }
		} else if (location.hash === "") {
			this.state = { ...Browse.Null, cid: match[0] }
		} else {
			this.state = {
				...Browse.Null,
				cid: match[0],
				focus: location.hash.slice(1),
			}
		}

		history.replaceState(
			{ index: this.state.index, cid: this.state.cid, focus: this.state.focus },
			title,
			url +
				(this.state.cid === null
					? this.state.index === null
						? ""
						: "?index=" + encodeURIComponent(this.state.index)
					: "?" + this.state.cid) +
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
			this.listGraphs(this.state.index)
		}
	}

	componentDidUpdate(prevProps, prevState) {
		if (
			this.state.cid === null &&
			(this.state.graphs === null || this.state.index !== prevState.index)
		) {
			this.listGraphs(this.state.index)
		}
	}

	async listGraphs(index) {
		const urls = await listGraphs(Browse.PageSize + 1, index)
		this.setState({ graphs: urls })
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

	handleClick = ({ target: { value: index } }) => {
		history.pushState(
			{ cid: null, focus: null, index },
			title,
			url + "?index=" + encodeURIComponent(index)
		)
		this.setState({ index })
	}

	render() {
		const { graphs, cid, focus } = this.state
		if (cid !== null) {
			return (
				<Message
					path={"/ipfs/" + cid}
					focus={focus}
					onFocus={this.handleFocus}
				/>
			)
		} else if (graphs === null) {
			return <p>Loading...</p>
		} else if (graphs.length === 0) {
			return <p>No graphs found</p>
		} else {
			return <ul>{graphs.map(this.renderGraph)}</ul>
		}
	}

	renderGraph = (url, index) => {
		if (url === null) {
			return null
		} else if (index === Browse.PageSize) {
			return (
				<li key={url}>
					<a href={`?index=${encodeURIComponent(url)}`}>Next page</a>
				</li>
			)
		} else {
			const path = url.split("/").pop()
			const [cid] = path.split("#")
			return (
				<li key={url}>
					<div>
						<span>{url}</span>
						<a href={`?${path}`}>View</a> |{" "}
						<a href={`${Browse.Gateway}/ipfs/${cid}`}>Download</a>
					</div>
				</li>
			)
		}
	}
}

ReactDOM.render(<Browse />, main)
