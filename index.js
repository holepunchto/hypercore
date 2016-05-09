var crypto = require('crypto')
var subleveldown = require('subleveldown')
var bulk = require('bulk-write-stream')
var collect = require('stream-collector')
var from = require('from2')
var feed = require('./lib/feed')
var messages = require('./lib/messages')
var hash = require('./lib/hash')
var replicate = require('./lib/replicate')

module.exports = Hypercore

function Hypercore (db, opts) {
  if (!(this instanceof Hypercore)) return new Hypercore(db, opts)
  if (!opts) opts = {}

  this.id = opts.id || crypto.randomBytes(32)

  this._db = db // TODO: needs levelup-defaults to force binary?
  this._nodes = subleveldown(db, 'nodes', {valueEncoding: messages.Node})
  this._data = subleveldown(db, 'data', {valueEncoding: 'binary'})
  this._signatures = subleveldown(db, 'signatures', {valueEncoding: 'binary'})
  this._feeds = subleveldown(db, 'feeds', {valueEncoding: messages.Feed})
  this._bitfields = subleveldown(db, 'bitfields', {valueEncoding: 'binary'})
  this._storage = opts.storage || null
}

Hypercore.publicId = Hypercore.prototype.publicId = function (key) {
  return hash.publicId(key)
}

Hypercore.prototype.replicate =
Hypercore.prototype.createReplicationStream = function (opts) {
  return replicate(this, opts)
}

Hypercore.prototype.createFeed = function (key, opts) {
  if (typeof key === 'string') key = Buffer(key, 'hex')
  if (key && !Buffer.isBuffer(key)) return this.createFeed(null, key)
  if (!opts) opts = {}
  if (!opts.key) opts.key = key
  opts.live = key ? !!opts.live : opts.live !== false // default to live feeds
  return feed(this, opts)
}

Hypercore.prototype.stat = function (key, cb) {
  var self = this
  this._feeds.get(hash.publicId(key).toString('hex'), function (_, feed) {
    if (feed) return cb(null, feed)
    self._feeds.get(key.toString('hex'), function (_, feed) {
      if (feed) return cb(null, feed)
      cb(new Error('Feed not found'))
    })
  })
}

Hypercore.prototype.list = function (cb) {
  var stream = this._feeds.createValueStream({
    valueEncoding: {
      asBuffer: true,
      decode: function (key) {
        return messages.Feed.decode(key).key
      }
    }
  })

  return collect(stream, cb)
}

Hypercore.prototype.createWriteStream = function (key, opts) {
  var feed = this.createFeed(key, opts)
  var stream = bulk.obj(write, flush)
  return patch(stream, feed)

  function write (buffers, cb) {
    feed.append(buffers, cb)
  }

  function flush (cb) {
    feed.finalize(function (err) {
      if (err) return cb(err)
      feed.close(function (err) {
        if (err) return cb(err)
        patch(stream, feed)
        cb()
      })
    })
  }
}

Hypercore.prototype.createReadStream = function (key, opts) {
  if (!opts) opts = {}
  var offset = 0
  var feed = this.createFeed(key, opts)
  var live = !!opts.live
  var stream = from.obj(read)
  return patch(stream, feed)

  function read (size, cb) {
    if (!live && feed.blocks && offset === feed.blocks) return cb(null, null)
    feed.get(offset++, cb)
  }
}

function patch (stream, feed) {
  stream.feed = feed
  stream.key = feed.key
  stream.publicId = feed.publicId
  stream.live = feed.live
  return stream
}
