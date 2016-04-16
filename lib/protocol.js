var crypto = require('crypto')
var stream = require('readable-stream')
var lpstream = require('length-prefixed-stream')
var varint = require('varint')
var duplexify = require('duplexify')
var inherits = require('inherits')
var encryption = require('sodium-encryption')
var increment = require('increment-buffer')
var messages = require('./messages')
var hash = require('./hash')

module.exports = Protocol

var KEEP_ALIVE = Buffer([0])
var CLOSE = Buffer(0)

var TYPES = [
  messages.Have,
  messages.Want,
  messages.Request,
  messages.Response,
  messages.Cancel,
  null, // pause
  null // resume
]

function Protocol (opts, handler) {
  if (!(this instanceof Protocol)) return new Protocol(opts, handler)
  if (!opts) opts = {}
  if (typeof opts === 'function') {
    handler = opts
    opts = {}
  }
  if (!handler) handler = noop

  duplexify.call(this)

  var self = this

  this.id = opts.id || crypto.randomBytes(32)
  this.remoteId = null
  this.encrypted = opts.encrypted !== false

  this.extensions = opts.extensions || handler.extensions || []
  this._remoteExtensions = new Array(this.extensions.length)
  this._localExtensions = new Array(this.extensions.length)
  for (var i = 0; i < this.extensions.length; i++) {
    this._remoteExtensions[i] = this._localExtensions[i] = -1
  }

  this._keepAliveInterval = null
  this._keepAlive = 0
  this._remoteKeepAlive = 0

  this._channels = {}
  this._local = []
  this._remote = []
  this._first = true
  this._firstFeed = null
  this._handler = handler

  this._encode = stream.Readable()
  this._encode._read = noop
  this._decode = lpstream.decode({limit: 5 * 1024 * 1024, allowEmpty: true})
  this._decode.on('data', parse)

  this.setReadable(this._encode)
  this.setWritable(this._decode)

  this.on('end', this.destroy)
  this.on('finish', this.destroy)
  this.on('close', this._ondestroy)

  function parse (data) {
    self._parse(data)
  }
}

inherits(Protocol, duplexify)

Protocol.prototype.add = function (feed, cb) {
  if (!cb) cb = noop
  var self = this

  if (feed.feeds || feed.feed) {
    var feeds = feed.feeds || [feed.feed]
    for (var i = 0; i < feeds.length; i++) this.add(feeds[i])
    return
  }

  if (!this._firstFeed) this._firstFeed = feed
  if (this._firstFeed) this._firstFeed.open(open) // ensure first feed is open
  else open()

  function open (err) {
    if (err && self._firstFeed === feed) self.destroy(new Error('First feed did not open'))
    if (self.destroyed) return cb(new Error('Stream destroyed'))
    if (err) return cb(err)
    feed.open(onopen)
  }

  function onopen (err) {
    if (self.destroyed) return cb('Stream destroyed')
    if (err) return cb(err)
    self.open(feed.key, feed.publicId, feed)
    cb()
  }
}

Protocol.prototype.remove = function (feed, cb) {
  if (!cb) cb = noop
  var self = this

  if (feed.feeds || feed.feed) {
    var feeds = feed.feeds || [feed.feed]
    for (var i = 0; i < feeds.length; i++) this.remove(feeds[i])
    return
  }

  if (this._firstFeed) this._firstFeed.open(open) // ensure first feed is open
  else open()

  function open (err) {
    if (self.destroyed) return cb(new Error('Stream destroyed'))
    if (err) return cb(err)
    feed.open(onopen)
  }

  function onopen (err) {
    if (self.destroyed) return cb('Stream destroyed')
    if (err) return cb(err)

    self._close(self._channels[feed.publicId.toString('hex')])
    cb()
  }
}

Protocol.prototype.open = function (key, publicId, feed) {
  if (!publicId) publicId = hash.publicId(key)

  var keyHex = publicId.toString('hex')
  var channel = this._channels[keyHex]

  if (!channel) channel = this._channels[keyHex] = new Channel(publicId)
  if (channel.local > -1) return channel.local

  channel.key = key
  channel.feed = feed
  channel.local = this._local.indexOf(null)

  var shouldInc = false

  if (this._first) {
    channel.protocol = messages.ProtocolHandshake.encode({peerId: this.id, extensions: this.extensions})
    if (this.encrypted) {
      channel.protocol = encryption.encrypt(channel.protocol, channel.nonce, key)
      shouldInc = true
    }
  }

  if (channel.local === -1) channel.local = this._local.push(null) - 1
  this._local[channel.local] = channel

  this._sendRaw(channel.local, messages.Handshake.encode(channel))
  if (shouldInc) increment(channel.nonce)

  if (this._first) {
    this._first = false
    this._decode.resume()
    this._openMaybe(channel, true)
  } else {
    this._openMaybe(channel, false)
  }

  return channel.local
}

Protocol.prototype.sendExtension = function (local, type, message) {
  this.send(local, type + 64, message)
}

Protocol.prototype.send = function (local, type, message) {
  if (local < 0) return

  var channel = this._local[local]
  if (!channel) return

  var enc = type < TYPES.length ? TYPES[type] : null
  var buf = null

  if (enc) {
    buf = Buffer(enc.encodingLength(message) + 1)
    enc.encode(message, buf, 1)
  } else if (message) {
    buf = Buffer(message.length + 1)
    message.copy(buf, 1)
  } else {
    buf = Buffer(1)
  }

  buf[0] = type
  if (this.encrypted) {
    buf = encryption.encrypt(buf, channel.nonce, channel.key)
    increment(channel.nonce)
  }

  this._sendRaw(local, buf)
}

Protocol.prototype.close = function (local) {
  this._close(this._local[local])
  this._sendRaw(local, CLOSE)
}

Protocol.prototype._close = function (channel) {
  if (!channel) return
  if (channel.local > -1) this._local[channel.local] = null
  if (channel.remote > -1) this._remote[channel.remote] = null
  delete this._channels[channel.publicId.toString('hex')]
  if (channel.handle && channel.handle.onclose) channel.handle.onclose()
}

Protocol.prototype.remoteSupports = function (id) {
  var i = typeof id === 'number' ? id : this.extensions.indexOf(id)
  return this._localExtensions[i] > -1
}

Protocol.prototype.setTimeout = function (ms, ontimeout) {
  if (ontimeout) this.once('timeout', ontimeout)

  var self = this

  this._keepAlive = 0
  this._remoteKeepAlive = 0

  clearInterval(this._keepAliveInterval)
  this._keepAliveInterval = setInterval(kick, (ms / 4) | 0)
  if (this._keepAliveInterval && typeof this._keepAliveInterval.unref === 'function') {
    this._keepAliveInterval.unref()
  }

  function kick () {
    self._kick()
  }
}

Protocol.prototype._kick = function () {
  if (this._remoteKeepAlive > 4) {
    clearInterval(this._keepAliveInterval)
    this.emit('timeout')
    return
  }

  this._remoteKeepAlive++
  if (this._keepAlive > 2) {
    this._encode.push(KEEP_ALIVE)
    this._keepAlive = 0
  } else {
    this._keepAlive++
  }

  for (var i = 0; i < this._local.length; i++) {
    var channel = this._local[i]
    if (channel && channel.handle && channel.handle.ontick) channel.handle.ontick()
  }
}

Protocol.prototype._sendRaw = function (local, buf) {
  var len = varint.encodingLength(local) + buf.length
  var box = Buffer(varint.encodingLength(len) + len)
  var offset = 0

  varint.encode(len, box, offset)
  offset += varint.encode.bytes
  varint.encode(local, box, offset)
  offset += varint.encode.bytes
  buf.copy(box, offset)
  this._keepAlive = 0

  return this._encode.push(box)
}

Protocol.prototype._parse = function (data) {
  this._remoteKeepAlive = 0
  if (!data.length || this.destroyed) return

  var remote = varint.decode(data, 0)
  var offset = varint.decode.bytes
  var channel = this._remote[remote]

  if (!channel) return this._onopen(remote, data, offset)

  if (this.encrypted) {
    if (!channel.key) return this.destroy(new Error('Channel is not open yet'))
    if (data.length < 16) return this.destroy(new Error('Invalid message'))
    data = encryption.decrypt(data.slice(offset), channel.remoteNonce, channel.key)
    increment(channel.remoteNonce)
    offset = 0
    if (!data) return this.destroy(new Error('Decryption failed'))
  }

  var handle = channel.handle
  var type = data[offset++]
  if (type > 127) return // max one-byte varint

  if (type >= 64) {
    var ext = this._remoteExtensions[type - 64]
    if (handle.onextension && ext > -1) handle.onextension(ext, data.slice(offset))
    return
  }

  var enc = type < TYPES.length ? TYPES[type] : null

  try {
    var message = enc ? enc.decode(data, offset) : null
  } catch (err) {
    return this.destroy(err)
  }

  if (handle.onmessage) handle.onmessage(type, message)
}

Protocol.prototype._onopen = function (remote, data, offset) {
  try {
    var open = messages.Handshake.decode(data, offset)
  } catch (err) {
    return
  }

  if (open.nonce.length !== 24 || open.publicId.length !== 32) return // not an open message

  var keyHex = open.publicId.toString('hex')
  var channel = this._channels[keyHex]

  if (!channel) channel = this._channels[keyHex] = new Channel(open.publicId)
  channel.remote = remote
  channel.remoteNonce = open.nonce
  channel.remoteProtocol = open.protocol

  this._remote[remote] = channel

  if (channel.local === -1) {
    if (this._first) this._decode.pause() // wait for at least one channel to be added
    this.emit('feed', open.publicId)
  }

  this._openMaybe(channel, false)
}

Protocol.prototype._openMaybe = function (channel, force) {
  if (!force && (channel.local === -1 || channel.remote === -1)) return

  if (channel.local > -1 && channel.remoteProtocol) {
    var data = channel.remoteProtocol

    if (!data || data.length < 16) return this.destroy(new Error('Protocol handshake needed on first channel'))

    if (this.encrypted) {
      data = encryption.decrypt(data, channel.remoteNonce, channel.key)
      if (!data) return this.destroy(new Error('Could not decrypt protocol handshake'))
      increment(channel.remoteNonce)
    }

    try {
      var handshake = messages.ProtocolHandshake.decode(data)
    } catch (err) {
      return this.destroy(err)
    }

    this._onhandshake(handshake)
    if (this.destroyed) return // incase the stream is destroyed while emitting handshake
  }

  if (!channel.handle) channel.handle = new this._handler(this, channel.local, channel.feed)
}

Protocol.prototype._ondestroy = function () {
  clearInterval(this._keepAliveInterval)
  var keys = Object.keys(this._channels)

  for (var i = 0; i < keys.length; i++) {
    var channel = this._channels[keys[i]]
    if (channel.handle && channel.handle.onclose) channel.handle.onclose()
  }
}

Protocol.prototype._onhandshake = function (handshake) {
  if (this.remoteId) return
  if (handshake.peerId.length !== 32) return this.destroy(new Error('Invalid remote peer id'))

  this.remoteId = handshake.peerId

  // extensions *must* be sorted
  var local = 0
  var remote = 0

  while (local < this.extensions.length && remote < handshake.extensions.length && remote < 64) {
    if (this.extensions[local] === handshake.extensions[remote]) {
      this._localExtensions[local] = remote
      this._remoteExtensions[remote] = local
      local++
      remote++
    } else if (this.extensions[local] < handshake.extensions[remote]) {
      local++
    } else {
      remote++
    }
  }

  this.emit('handshake')
}

function Channel (publicId) {
  this.key = null
  this.publicId = publicId
  this.handle = null
  this.feed = null

  this.local = -1
  this.remote = -1

  this.nonce = crypto.randomBytes(24)
  this.remoteNonce = null

  this.handshake = null
  this.remoteProtocol = null
}

function noop () {}
