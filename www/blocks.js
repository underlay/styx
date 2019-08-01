const variable = "variable"
const iri = "iri"
const node = "node"
const predicate = "predicate"

Blockly.Blocks[variable] = {
	init: function() {
		this.jsonInit({
			type: variable,
			message0: "∶%1 %2",
			args0: [
				{
					type: "field_input",
					name: "id",
					text: "b0",
				},
				{
					type: "input_statement",
					name: "properties",
					check: predicate,
				},
			],
			inputsInline: true,
			output: node,
			colour: 330,
			tooltip: "A blank node in RDF",
			helpUrl: "http://localhost:3000",
		})
	},
}

Blockly.Blocks[iri] = {
	init: function() {
		this.jsonInit({
			type: iri,
			message0: "〈 %1 〉 %2",
			args0: [
				{
					type: "field_input",
					name: "id",
					text: "http://...",
				},
				{
					type: "input_statement",
					name: "properties",
					check: predicate,
				},
			],
			inputsInline: true,
			output: node,
			colour: 30,
			tooltip: "A blank node in RDF",
			helpUrl: "http://localhost:3000",
		})
	},
}

Blockly.Blocks[predicate] = {
	init: function() {
		this.jsonInit({
			type: predicate,
			message0: "%1 = %2",
			args0: [
				{
					type: "field_input",
					name: "id",
					text: "schema:name",
				},
				{
					type: "input_value",
					name: "value",
					check: ["String", "Boolean", "Number", node],
				},
			],
			previousStatement: predicate,
			nextStatement: predicate,
			colour: 260,
			tooltip: "A constraint on a blank node",
			helpUrl: "http://localhost:3000",
		})
	},
}
