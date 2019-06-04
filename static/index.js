// create the editor
const queryContainer = document.getElementById("query")
const queryEditor = new JSONEditor(queryContainer, {
	mode: "code",
	modes: [],
})

// set json
queryEditor.set({
	"@context": {
		"@vocab": "http://schema.org/",
		parent: { "@reverse": "children" },
	},
	"@type": "Person",
	birthDate: {},
	parent: {
		name: "Joel",
	},
})

const resultContainer = document.getElementById("result")
const resultEditor = new JSONEditor(resultContainer, {
	mode: "view",
	modes: [],
})

document.getElementById("execute-query").addEventListener("click", () => {
	const body = JSON.stringify(queryEditor.get())
	console.log("got body", body)
	fetch("/query", { method: "POST", body })
		.then(res => res.json())
		.then(json => {
			resultEditor.set(json)
			resultEditor.expandAll()
		})
})

const ingestContainer = document.getElementById("ingest")
const ingestEditor = new JSONEditor(ingestContainer, {
	mode: "code",
	modes: [],
})

// set json
ingestEditor.set({
	"@context": {
		"@vocab": "http://schema.org/",
		parent: { "@reverse": "children" },
	},
	"@type": "Person",
	birthDate: {},
	parent: {
		name: "Joel",
	},
})

document.getElementById("execute-ingest").addEventListener("click", () => {
	const body = JSON.stringify(ingestEditor.get())
	console.log("got body", body)
	fetch("/ingest", { method: "POST", body })
		.then(res => res.json())
		.then(json => {
			console.log("JSON", json)
		})
})
