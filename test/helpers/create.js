var hypercore = require('../../')
var memdb = require('memdb')

module.exports = function create (opts) {
  // ensure hypercore overwrites the db's default encoding
  return hypercore(memdb({keyEncoding: 'json'}), opts)
}
