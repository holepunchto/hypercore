var hypercore = require('../..')
var tmp = require('tmp')

module.exports = function create (key, opts) {
  var tmpobj = tmp.dirSync({unsafeCleanup: true})
  var feed = hypercore(tmpobj.name, key, opts)
  return feed
}
