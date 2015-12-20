var from = require('from2')

module.exports = readStream

function readStream (core, id, opts) {
  if (!opts) opts = {}
  var start = opts.start || 0
  var limit = opts.limit || Infinity
  var feed = core.get(id, opts)
  var stream = from.obj(read)
  stream.id = id
  stream.blocks = feed.blocks
  feed.ready(onready)
  return stream

  function read (size, cb) {
    if (limit-- === 0) return cb(null, null)
    feed.get(start++, cb)
  }

  function onready () {
    stream.blocks = feed.blocks
    stream.emit('ready')
  }
}
