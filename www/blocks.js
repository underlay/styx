const node = "node"
const blank = "blank"
const iri = "iri"
const predicate = "predicate"
const blankPredicate = "blank-predicate"
const literal = "literal"

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
					check: [predicate, blankPredicate],
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
					check: ["String", "Boolean", "Number", literal, iri, blank, node],
				},
			],
			previousStatement: [predicate, blankPredicate],
			nextStatement: [predicate, blankPredicate],
			colour: 260,
		})
	},
}

Blockly.Blocks[blankPredicate] = {
	init: function() {
		this.jsonInit({
			type: blankPredicate,
			message0: "_:%1 %2",
			args0: [
				{
					type: "field_input",
					name: "id",
				},
				{
					type: "input_value",
					name: "object",
					check: ["String", "Boolean", "Number", literal, iri, blank, node],
				},
			],
			previousStatement: [predicate, blankPredicate],
			nextStatement: [predicate, blankPredicate],
			colour: 180,
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

Blockly.Blocks[literal] = {
	init: function() {
		this.jsonInit({
			type: literal,
			message0: '"%1"',
			message1: "‹%1›",
			args0: [
				{
					type: "field_input",
					name: "value",
				},
			],
			args1: [
				{
					type: "field_input",
					name: "type",
				},
			],
			output: literal,
			colour: 60,
		})
	},
}
