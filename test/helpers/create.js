const hypercore = require('../..')
const ram = require('random-access-memory')

module.exports = function create (key, opts) {
  return hypercore(ram, key, opts)
}
