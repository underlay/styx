import React from "react"
import ReactDOM from "react-dom"

import { object, predicateQuery, leafQuery } from "./query"

const main = document.querySelector("main")

const title = document.title
const url = location.origin + location.pathname

class Browse extends React.Component {
	static LeafPageSize = 1
	static PredicatePageSize = 3
	static Examples = [
		"<http://schema.org/DigitalDocument>",
		'"text/plain"',
		"<dweb:/ipfs/QmfCtdbfajVvzTsoUDMLBWvJtnh6mpA6SQTBwrMWCEhmdt>",
	]

	static Null = {
		value: null,
		id: null,
		predicates: null,
		leaves: null,
	}

	constructor(props) {
		super(props)
		const id = decodeURIComponent(location.hash.slice(1))
		if (object.test(id)) {
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
			if (object.test(id)) {
				this.setState({ ...Browse.Null, id }, () => this.fetchPredicates())
				history.replaceState({ id: this.state.id }, title, url + location.hash)
			} else {
				this.setState({ ...Browse.Null, value: this.state.value || "" })
				history.replaceState({ id: this.state.id }, title, url)
			}
		})

		if (this.state.id !== null) {
			this.fetchPredicates()
		}
	}

	async fetchPredicates() {
		const predicates = await predicateQuery(
			this.state.id,
			Browse.PredicatePageSize + 1,
			null,
			null
		)
		this.setState({ predicates }, () => this.fetchLeaves())
	}

	async fetchLeaves() {
		const { id, predicates } = this.state
		const leaves = await leafQuery(
			id,
			predicates.map(p => p.slice(0, Browse.PredicatePageSize)),
			Browse.LeafPageSize + 1,
			null,
			null
		)
		this.setState({ leaves })
	}

	handleSubmit = event => {
		event.preventDefault()
		if (object.test(this.state.value)) {
			location.hash = `#${encodeURIComponent(this.state.value)}`
		}
	}

	handleChange = ({ target: { value } }) => this.setState({ value })

	render() {
		const { id, value } = this.state
		if (id === null) {
			const disabled = !object.test(value)
			return (
				<div>
					<p>Enter an RDF term:</p>
					<form onSubmit={this.handleSubmit}>
						<input type="text" value={value} onChange={this.handleChange} />
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
				<table>
					<tbody>{this.renderRows()}</tbody>
				</table>
			)
		}
	}

	renderRows() {
		const { id, predicates, leaves } = this.state
		if (predicates === null && leaves === null) {
			return (
				<React.Fragment>
					<tr>
						<td colSpan="5">{id}</td>
					</tr>
					<tr>
						<td colSpan="5">Loading...</td>
					</tr>
				</React.Fragment>
			)
		} else if (predicates !== null && leaves === null) {
			const [l, r] = predicates
			const height = Math.min(
				Math.max(l.length, r.length),
				Browse.PredicatePageSize
			)
			const rows = []
			for (let i = 0; i < height; i++) {
				rows.push(
					<tr key={i}>
						{i < l.length ? (
							<React.Fragment>
								<td>{this.renderValue(l[i][1])}</td>
								<td>{this.renderPredicate(l[i][0])}</td>
							</React.Fragment>
						) : i === l.length ? (
							<td colSpan="2" rowSpan={height - l.length}></td>
						) : null}
						{i === 0 ? <td rowSpan={height}>{id}</td> : null}
						{i < r.length ? (
							<React.Fragment>
								<td>{this.renderPredicate(r[i][0])}</td>
								<td>{this.renderValue(r[i][1])}</td>
							</React.Fragment>
						) : i === r.length ? (
							<td colSpan="2" rowSpan={height - r.length}></td>
						) : null}
					</tr>
				)
			}

			return rows
		} else if (predicates !== null && leaves !== null) {
			const [lp, rp] = predicates
			const [ll, rl] = leaves
			const leftLeafCells = []
			const leftPredicateCells = []
			const rightLeafCells = []
			const rightPredicateCells = []

			for (let i = 0; i < lp.length; i++) {
				if (i === Browse.PredicatePageSize) {
					leftLeafCells.push(<td></td>)
					leftPredicateCells.push(
						<td>
							<button>...more</button>
						</td>
					)
				} else {
					for (let j = 0; j < ll[i].length; j++) {
						if (j === 0) {
							leftPredicateCells.push(
								<td rowSpan={ll[i].length}>{this.renderPredicate(lp[i][0])}</td>
							)
						} else {
							leftPredicateCells.push(null)
						}
						if (j === Browse.LeafPageSize) {
							leftLeafCells.push(
								<td>
									<button>...more</button>
								</td>
							)
						} else {
							leftLeafCells.push(<td>{this.renderValue(ll[i][j])}</td>)
						}
					}
				}
			}

			for (let i = 0; i < rp.length; i++) {
				if (i === Browse.PredicatePageSize) {
					rightLeafCells.push(<td></td>)
					rightPredicateCells.push(
						<td>
							<button>...more</button>
						</td>
					)
				} else {
					for (let j = 0; j < rl[i].length; j++) {
						if (j === 0) {
							rightPredicateCells.push(
								<td rowSpan={rl[i].length}>{this.renderPredicate(rp[i][0])}</td>
							)
						} else {
							rightPredicateCells.push(null)
						}
						if (j === Browse.LeafPageSize) {
							rightLeafCells.push(
								<td>
									<button>...more</button>
								</td>
							)
						} else {
							rightLeafCells.push(<td>{this.renderValue(rl[i][j])}</td>)
						}
					}
				}
			}

			if (leftLeafCells.length > rightLeafCells.length) {
				const rowSpan = leftLeafCells.length - rightLeafCells.length
				rightPredicateCells.push(<td colSpan="2" rowSpan={rowSpan}></td>)
				rightLeafCells.push(null)
				for (let i = 1; i < rowSpan; i++) {
					rightPredicateCells.push(null)
					rightLeafCells.push(null)
				}
			} else if (leftLeafCells.length < rightLeafCells.length) {
				const rowSpan = rightLeafCells.length - leftLeafCells.length
				leftLeafCells.push(<td colSpan="2" rowSpan={rowSpan}></td>)
				leftPredicateCells.push(null)
				for (let i = 1; i < rowSpan; i++) {
					leftPredicateCells.push(null)
					leftLeafCells.push(null)
				}
			}

			const rows = []
			const height = Math.max(rightLeafCells.length, leftLeafCells.length)
			for (let i = 0; i < height; i++) {
				const row = (
					<tr key={i}>
						{leftLeafCells[i]}
						{leftPredicateCells[i]}
						{i === 0 ? <td rowSpan={height}>{id}</td> : null}
						{rightPredicateCells[i]}
						{rightLeafCells[i]}
					</tr>
				)
				rows.push(row)
			}
			return rows
		}
	}

	renderPredicate(id) {
		return (
			<span>
				<span className="syntax">&lt;</span>
				{id}
				<span className="syntax">&gt;</span>
			</span>
		)
	}

	renderValue(value) {
		if (typeof value["@id"] === "string") {
			return (
				<React.Fragment>
					<span className="syntax">&lt;</span>
					<a href={`#${encodeURIComponent(`<${value["@id"]}>`)}`}>
						{value["@id"]}
					</a>
					<span className="syntax">&gt;</span>
				</React.Fragment>
			)
		} else {
			return <span>{JSON.stringify(value["@value"])}</span>
		}
	}
}

ReactDOM.render(<Browse />, main)
