var webpack = require('webpack');
module.exports = {
  entry: [
    "./client/app.js"
  ],
  output: {
    path: __dirname + '/app/static',
    filename: "bundle.js"
  },
  module: {
    loaders: [
      {
        test: /\.js?$/,
        loader: 'babel-loader',
      },
      {test: /\.css$/, loader: 'style-loader!css-loader'}
    ]
  },
  plugins: [
  ]
};
