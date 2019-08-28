const main = document.querySelector("main")

const explore =
	window.location.hostname === "localhost"
		? "http://localhost:8000"
		: "https://underlay.github.io/explore"

const context = {
	dcterms: "http://purl.org/dc/terms/",
	prov: "http://www.w3.org/ns/prov#",
	rdfs: "http://www.w3.org/2000/01/rdf-schema#",
	rdf: "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
	xsd: "http://www.w3.org/2001/XMLSchema#",
	u: "http://underlay.mit.edu/ns#",
}

const query = {
	"@context": context,
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
}

async function stuff() {
	const body = JSON.stringify(query)
	const res = await fetch("/", {
		method: "POST",
		headers: { "Content-Type": "application/ld+json" },
		body,
	})

	const ul = document.createElement("ul")
	if (res.status === 200) {
		const json = await res.json()
		const [
			{
				"@graph": [
					{
						"http://www.w3.org/ns/prov#value": [{ "@list": values }],
					},
				],
				"http://underlay.mit.edu/ns#instanceOf": [{ "@id": q }],
			},
		] = await jsonld.expand(json)
		let path,
			container = null
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
			const url = new URL(id)
			const fragment = url.hash || "#"
			const li = document.createElement("li")
			const a = document.createElement("a")
			const cid = url.pathname.split("/").pop()
			const href = explore + "#" + cid
			a.setAttribute("href", href + fragment)
			a.textContent = fragment
			li.appendChild(a)
			if (url.pathname === path && container !== null) {
				container.appendChild(li)
			} else {
				path = url.pathname
				container = document.createElement("ul")
				container.appendChild(li)
				const message = document.createElement("li")
				const messageAnchor = document.createElement("a")
				messageAnchor.setAttribute("href", href)
				messageAnchor.textContent = cid
				const messageDate = document.createElement("div")
				messageDate.textContent = new Date().toLocaleString()
				message.appendChild(messageAnchor)
				message.appendChild(messageDate)
				message.appendChild(container)
				ul.appendChild(message)
			}
		}
		main.removeChild(main.firstElementChild)
		main.appendChild(ul)
	} else {
		console.error(res.statusText)
	}
}

stuff()
