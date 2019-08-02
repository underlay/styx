const node = "node"
const blank = "blank"
const iri = "iri"
const predicate = "predicate"

Blockly.Blocks[node] = {
	init: function() {
		this.jsonInit({
			type: node,
			message0: "%1 %2",
			inputsInline: true,
			args0: [
				{
					type: "input_value",
					name: "subject",
					check: [iri, blank],
				},
				{
					type: "input_statement",
					name: "predicate",
					check: predicate,
				},
			],
			inputsInline: true,
			output: node,
			colour: 100,
		})
	},
}

Blockly.Blocks[predicate] = {
	init: function() {
		this.jsonInit({
			type: predicate,
			message0: "〈%1〉%2",
			args0: [
				{
					type: "field_input",
					name: "id",
				},
				{
					type: "input_value",
					name: "object",
					check: ["String", "Boolean", "Number", iri, blank, node],
				},
			],
			previousStatement: predicate,
			nextStatement: predicate,
			colour: 260,
		})
	},
}

Blockly.Blocks[blank] = {
	init: function() {
		this.jsonInit({
			type: blank,
			message0: "_∶%1",
			args0: [
				{
					type: "field_input",
					name: "id",
				},
			],
			output: blank,
			colour: 330,
		})
	},
}

Blockly.Blocks[iri] = {
	init: function() {
		this.jsonInit({
			type: iri,
			message0: "〈%1〉",
			args0: [
				{
					type: "field_input",
					name: "id",
				},
			],
			output: iri,
			colour: 30,
		})
	},
}
