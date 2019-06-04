const options = {
	mode: "code",
	modes: [],
	colorPicker: false,
	timestampTag: false,
	enableTransform: false,
	statusBar: false,
	autocomplete: {
		caseSensitive: true,
		getOptions: (text, path, input, editor) => [
			"@id",
			"@type",
			"@graph",
			"@list",
			"@set",
			"@context",
			"@vocab",
			"@container",
			"@version",
		],
	},
}

// create the editor
const queryContainer = document.getElementById("query")
const queryEditor = new JSONEditor(queryContainer, options)

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
	fetch("/query", { method: "POST", body })
		.then(async res => {
			if (res.status === 200) {
				const json = await res.json()
				resultEditor.set(json)
				resultEditor.expandAll()
			} else {
				const text = await res.text()
				console.error(`${res.statusText}: ${text}`)
			}
		})
		.catch(err => console.error(err))
})

const ingestContainer = document.getElementById("ingest")
const ingestEditor = new JSONEditor(ingestContainer, options)

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
