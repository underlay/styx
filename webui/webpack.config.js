const path = require("path")
const CopyPlugin = require("copy-webpack-plugin")

const options = {
	presets: ["@babel/preset-env", "@babel/preset-react"],
	plugins: ["@babel/plugin-proposal-class-properties"],
}

const exclude = /(?:\.min\.js$|dist\/)/

module.exports = {
	entry: {
		"www/directory/index": [
			"@babel/polyfill",
			path.resolve(__dirname, "src", "directory.jsx"),
		],
		"www/query/index": [
			"@babel/polyfill",
			path.resolve(__dirname, "src", "query.jsx"),
		],
		"www/browse/index": [
			"@babel/polyfill",
			path.resolve(__dirname, "src", "browse.jsx"),
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
				to: path.resolve(__dirname, "www", "explore.css"),
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

	optimization: {
		splitChunks: {
			cacheGroups: {
				blockly: {
					test: /[\\/]node_modules[\\/](blockly)[\\/]/,
					name: "www/blockly",
					chunks: "all",
				},
				commons: {
					name: "www/commons",
					chunks: "initial",
					minChunks: 2,
				},
			},
		},
	},
}
