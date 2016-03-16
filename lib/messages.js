var fs = require('fs')
var path = require('path')
var protobuf = require('protocol-buffers')

module.exports = protobuf(fs.readFileSync(path.join(__dirname, '../schema.proto'), 'utf-8'))
