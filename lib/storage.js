var flat = require('flat-tree')

module.exports = Storage

function Storage (feed, store) {
  if (!(this instanceof Storage)) return new Storage(feed, store)
  this.store = store
  this.feed = feed
}

Storage.prototype.open = function (cb) {
  if (this.store.open) this.store.open(cb)
  else cb()
}

Storage.prototype.append = function (block, datas, cb) {
  if (!datas.length) return cb(null)
  lookup(this, block, datas.length === 1 ? datas[0] : Buffer.concat(datas), cb)
}

Storage.prototype.get = function (block, cb) {
  lookup(this, block, null, cb || noop)
}

Storage.prototype.put = function (block, data, cb) {
  lookup(this, block, data, cb || noop)
}

Storage.prototype.close = function (cb) {
  if (this.store.close) this.store.close(cb)
  else cb()
}

function noop () {}

function lookup (self, block, data, cb) {
  var blk = block * 2
  var prefix = self.feed._prefix
  var nodes = self.feed._core._nodes
  var prev = flat.fullRoots(2 * block)
  var missing = prev.length
  var offset = 0
  var error = null
  var length = 0

  if (!data) {
    missing++
    nodes.get(prefix + (2 * block), onnode)
  }
  for (var i = 0; i < prev.length; i++) {
    nodes.get(prefix + prev[i], onnode)
  }

  if (!missing && data) self.store.write(0, data, cb)

  function onnode (err, node) {
    if (err) error = err
    else if (node.index === blk) length = node.size
    else offset += node.size

    if (--missing) return
    if (error) return cb(error)

    if (data) self.store.write(offset, data, cb)
    else self.store.read(offset, length, cb)
  }
}
