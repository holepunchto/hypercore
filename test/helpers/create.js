var hypercore = require('../../')
var memdb = require('memdb')

module.exports = function create () {
  return hypercore(memdb())
}
