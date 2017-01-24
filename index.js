var randomBytes = require('randombytes')
var defaults = require('levelup-defaults')
var subleveldown = require('subleveldown')
var bulk = require('bulk-write-stream')
var collect = require('stream-collector')
var from = require('from2')
var feed = require('./lib/feed')
var messages = require('./lib/messages')
var hash = require('./lib/hash')
var replicate = require('./lib/replicate')
var passthroughEncoding = require('passthrough-encoding')

module.exports = Hypercore

function Hypercore (db, opts) {
  if (!(this instanceof Hypercore)) return new Hypercore(db, opts)
  if (!opts) opts = {}

  this.id = opts.id || randomBytes(32)

  this._open = {}
  this._db = defaults(db, {keyEncoding: 'utf8'})
  this._nodes = subleveldown(db, 'nodes', {valueEncoding: messages.Node})
  this._data = subleveldown(db, 'data', {valueEncoding: 'binary'})
  this._signatures = subleveldown(db, 'signatures', {valueEncoding: 'binary'})
  this._feeds = subleveldown(db, 'feeds', {valueEncoding: messages.Feed})
  this._bitfields = subleveldown(db, 'bitfields', {valueEncoding: 'binary'})
  this._storage = opts.storage || null
  this._valueEncoding = opts.valueEncoding || passthroughEncoding
}

Hypercore.discoveryKey = Hypercore.prototype.discoveryKey = function (key) {
  return hash.discoveryKey(key)
}

Hypercore.prototype.replicate = function (opts) {
  return replicate(this, null, opts)
}

Hypercore.prototype.unreplicate = function (feed, stream) {
  return replicate.unreplicate(this, feed, stream)
}

Hypercore.prototype.createFeed = function (key, opts) {
  if (typeof key === 'string') key = new Buffer(key, 'hex')
  if (key && !Buffer.isBuffer(key)) return this.createFeed(null, key)
  if (!opts) opts = {}

  opts.key = key || opts.key
  if (typeof opts.key === 'string') opts.key = Buffer(opts.key, 'hex')
  if (!opts.valueEncoding) opts.valueEncoding = this._valueEncoding
  opts.live = opts.key ? !!opts.live : opts.live !== false // default to live feeds

  // TODO: do not return the same feed but just have a small pool of shared state
  // Or! do not have any shared state in general.

  var old = opts.key && (this._open[opts.key.toString('hex')] || this._open[hash.discoveryKey(opts.key).toString('hex')])
  if (old) return old

  var f = feed(this, opts)
  if (f.discoveryKey) {
    this._open[f.discoveryKey.toString('hex')] = f
    f.on('close', onclose)
  }
  return f
}

Hypercore.prototype.stat = function (key, cb) {
  var self = this
  this._feeds.get(hash.discoveryKey(key).toString('hex'), function (_, feed) {
    if (feed) return cb(null, feed)
    self._feeds.get(key.toString('hex'), function (_, feed) {
      if (feed) return cb(null, feed)
      cb(new Error('Feed not found'))
    })
  })
}

Hypercore.prototype.list = function (opts, cb) {
  if (typeof opts === 'function') {
    cb = opts
    opts = null
  }
  var stream = this._feeds.createValueStream({
    valueEncoding: {
      asBuffer: true,
      decode: function (key) {
        var value = messages.Feed.decode(key)
        if (opts && opts.values) {
          return value
        }
        return value.key
      }
    }
  })

  return collect(stream, cb)
}

Hypercore.prototype.createWriteStream = function (key, opts) {
  if (!opts) opts = {}

  var feed = (opts && opts.feed) || (isFeed(key) ? key : this.createFeed(key, opts))
  var create = opts.objectMode === false ? bulk : bulk.obj
  var stream = create(write, flush)
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

  var offset = opts.start || 0
  var end = opts.end || -1
  var feed = opts.feed || (isFeed(key) ? key : this.createFeed(key, opts))
  var live = !!opts.live
  var create = opts.objectMode === false ? from : from.obj
  var stream = create(read)
  var range = feed.prioritize({prioritize: 3, start: offset, end: end, linear: true})

  stream.on('close', cleanup)
  stream.on('end', cleanup)

  return patch(stream, feed)

  function cleanup () {
    feed.unprioritize(range)
  }

  function read (size, cb) {
    if (offset === end) return cb(null, null)
    if (!live && feed.blocks && offset === feed.blocks) return cb(null, null)
    feed.get(offset++, cb)
  }
}

function patch (stream, feed) {
  stream.feed = feed
  stream.key = feed.key
  stream.discoveryKey = feed.discoveryKey
  stream.live = feed.live
  return stream
}

function isFeed (feed) {
  return feed && Buffer.isBuffer(feed.key) && feed.key.length === 32 && typeof feed.open === 'function'
}

function onclose () {
  var core = this._core
  if (this.discoveryKey && core._open[this.discoveryKey.toString('hex')] === this) {
    delete core._open[this.discoveryKey.toString('hex')]
  }
}
