var bulk = require('bulk-write-stream')

module.exports = writeStream

function writeStream (core, opts) {
  var feed = core.add(opts)
  var stream = bulk.obj(write, flush)
  stream.id = null
  stream.blocks = 0
  return stream

  function write (batch, cb) {
    feed.append(batch, cb)
  }

  function flush (cb) {
    feed.finalize(function (err) {
      if (err) return cb(err)
      stream.id = feed.id
      stream.blocks = feed.blocks
      cb()
    })
  }
}
