var hypercore = require('../..')
var ram = require('random-access-memory')

module.exports = function create (key, opts) {
  if (key && !(typeof key === 'string' || Buffer.isBuffer(key))) return create(null, key)
  if (!opts) opts = {}
  if (!opts.storage) opts.storage = ram
  return hypercore(key, opts)
}
