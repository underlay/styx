const path = require("path")
const CopyPlugin = require("copy-webpack-plugin")

const options = {
	presets: ["@babel/preset-env", "@babel/preset-react"],
	plugins: ["@babel/plugin-proposal-class-properties"],
}

const exclude = /(?:\.min\.js$|dist\/)/

module.exports = {
	entry: {
		"browse/lib/index": [
			"@babel/polyfill",
			path.resolve(__dirname, "browse", "index.jsx"),
		],
		"query/lib/index": [
			"@babel/polyfill",
			path.resolve(__dirname, "query", "index.jsx"),
		],
	},

	output: {
		path: __dirname,
		filename: "[name].min.js",
	},

	resolve: {
		extensions: [".js", ".jsx", ".json"],
	},

	plugins: [
		new CopyPlugin([
			{
				from: path.resolve(__dirname, "node_modules", "explore", "index.css"),
				to: path.resolve(__dirname, "browse", "lib", "explore.css"),
			},
		]),
	],

	module: {
		rules: [
			{
				test: /\.jsx?$/,
				exclude,
				use: [{ loader: "babel-loader", options }],
			},
		],
	},
}
