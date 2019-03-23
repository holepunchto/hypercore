var hypercore = require('../..')
var tmp = require('tmp')

module.exports = function create (key, opts) {
  var tmpobj = tmp.dirSync({unsafeCleanup: true})
  return hypercore(tmpobj.name, key, opts)
}
