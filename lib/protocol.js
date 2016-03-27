var varint = require('varint')
var crypto = require('crypto')
var duplexify = require('duplexify')
var inherits = require('inherits')
var stream = require('readable-stream')
var lpstream = require('length-prefixed-stream')
var encryption = require('sodium-encryption')
var increment = require('increment-buffer')
var events = require('events')
var messages = require('./messages')

var MAX_MESSAGE_LENGTH = 5 * 1024 * 1024
var KEEP_ALIVE = Buffer([0])
var ENCODERS = [
  messages.Handshake,
  null, // close
  messages.Have,
  null, // pause
  null, // resume
  messages.Request,
  messages.Response,
  messages.Cancel
]

module.exports = Protocol

function Channel (protocol, publicId) {
  events.EventEmitter.call(this)

  this.publicId = publicId
  this.key = null
  this.feed = null
  this.closed = false

  this._encrypt = protocol._encrypt
  this._buffers = null
  this._offsets = null
  this._protocol = protocol
  this._nonce = null
  this._remoteNonce = null
  this._local = -1
  this._remote = -1
}

inherits(Channel, events.EventEmitter)

Channel.prototype._onopen = function (message, remote) {
  this._remote = remote
  this._protocol._remote[remote] = this
  this._remoteNonce = message.nonce
  this.emit('open')
}

Channel.prototype.handshake = function (handshake) {
  this._send(0, handshake)
}

Channel.prototype.close = function (err) {
  if (err) this.emit('warning', err)
  this.closed = true

  var protocol = this._protocol
  var keyHex = this.publicId.toString('hex')

  if (protocol._channels[keyHex] !== this) return

  if (this.key) this._send(1, null)

  delete protocol._channels[keyHex]
  if (this._remote > -1) protocol._remote[this._remote] = null
  if (this._local > -1) protocol._local[this._local] = null

  this.emit('close')

  if (!this._buffers) return
  for (var i = 0; i < this._buffers.length; i++) {
    this._protocol._parse(this._buffers[i])
  }
}

Channel.prototype.have = function (have) {
  this._send(2, have)
}

Channel.prototype.pause = function () {
  this._send(3, null)
}

Channel.prototype.resume = function () {
  this._send(4, null)
}

Channel.prototype.request = function (request) {
  this._send(5, request)
}

Channel.prototype.response = function (response) {
  this._send(6, response)
}

Channel.prototype.cancel = function (cancel) {
  this._send(7, cancel)
}

Channel.prototype.extension = function (type, buf) {
  this._send(type + 64, buf)
}

Channel.prototype._send = function (type, message) {
  if (this.closed) return

  var enc = type < ENCODERS.length ? ENCODERS[type] : null
  var tmp = Buffer(1 + (enc ? enc.encodingLength(message) : (message ? message.length : 0)))

  tmp[0] = type
  if (enc) enc.encode(message, tmp, 1)
  else if (message) message.copy(tmp, 1)

  if (this._encrypt) {
    tmp = encryption.encrypt(tmp, this._nonce, this.key)
    increment(this._nonce)
  }

  var len = varint.encodingLength(this._local) + tmp.length
  var buf = Buffer(varint.encodingLength(len) + len)
  var offset = 0

  varint.encode(len, buf, offset)
  offset += varint.encode.bytes

  varint.encode(this._local, buf, offset)
  offset += varint.encode.bytes

  tmp.copy(buf, offset)

  this._protocol._encode.push(buf)
  this._protocol._keepAlive = 0
}

Channel.prototype._parse = function (buf, offset) {
  if (!this.key) {
    if (!this._buffers) {
      this._buffers = []
      this._offsets = []
    }

    this._buffers.push(buf)
    this._offsets.push(offset)

    if (this._buffers.length > 16) return this._protocol.destroy()
    return
  }

  if (this._encrypt) {
    buf = encryption.decrypt(buf.slice(offset), this._remoteNonce, this.key)
    offset = 0
    increment(this._remoteNonce)
  }

  var type = buf[offset++]

  if (type >= 64 && type < 128) return this.emit('extension', type - 64, buf.slice(offset))

  var enc = ENCODERS[type]

  try {
    var message = enc && enc.decode(buf, offset)
  } catch (err) {
    this._protocol.destroy(err)
    return false
  }

  switch (type) {
    case 0: return this.emit('handshake', message)
    case 1: return this._onclose()
    case 2: return this.emit('have', message)
    case 3: return this.emit('pause')
    case 4: return this.emit('resume')
    case 5: return this.emit('request', message)
    case 6: return this.emit('response', message)
    case 7: return this.emit('cancel', message)
  }

  return false
}

Channel.prototype._onclose = function () {
  this.close()
  return true
}

Channel.prototype._open = function (feed) {
  if (this.feed) return
  this.feed = feed
  this.key = feed.key
  this._local = this._protocol._local.indexOf(null)
  if (this._local === -1) this._local = this._protocol._local.push(null) - 1
  this._protocol[this._local] = this
  this._nonce = crypto.randomBytes(24)

  var open = {publicId: feed.publicId, nonce: this._nonce}
  var len = varint.encodingLength(this._local) + messages.Open.encodingLength(open)
  var buf = Buffer(varint.encodingLength(len) + len)
  var offset = 0

  varint.encode(len, buf, offset)
  offset += varint.encode.bytes

  varint.encode(this._local, buf, offset)
  offset += varint.encode.bytes

  messages.Open.encode(open, buf, offset)
  offset += messages.Open.encode.bytes

  this._protocol._encode.push(buf)
  this._protocol._keepAlive = 0

  this._protocol.emit('channel', this)

  if (!this._buffers) return
  while (this._buffers.length) {
    this._parse(this._buffers.shift(), this._offsets.shift())
  }
}

function Protocol (opts) {
  if (!(this instanceof Protocol)) return new Protocol(opts)
  if (!opts) opts = {}
  duplexify.call(this)

  var self = this

  this._local = []
  this._remote = []
  this._channels = {}
  this._encrypt = opts.encrypt !== false

  this._keepAlive = 0
  this._remoteKeepAlive = 0
  this._interval = null

  this._encode = new stream.Readable()
  this._encode._read = noop
  this.setReadable(this._encode)

  this._decode = lpstream.decode({allowEmpty: true, limit: MAX_MESSAGE_LENGTH})
  this._decode.on('data', parse)
  this.setWritable(this._decode)

  this.on('end', this.destroy)
  this.on('finish', this.destroy)
  this.on('close', this._close)

  function parse (data) {
    self._parse(data)
  }
}

inherits(Protocol, duplexify)

Protocol.prototype.setTimeout = function (ms, ontimeout) {
  if (ontimeout) this.once('timeout', ontimeout)
  var self = this

  this._keepAlive = 0
  this._remoteKeepAlive = 0

  clearInterval(this._keepAliveInterval)
  this._keepAliveInterval = setInterval(kick, (ms / 4) | 0)
  if (this._keepAliveInterval) this._keepAliveInterval.unref()

  function kick () {
    if (self._remoteKeepAlive > 4) {
      clearInterval(self._keepAliveInterval)
      self.emit('timeout')
      return
    }

    self._remoteKeepAlive++
    if (self._keepAlive > 2) {
      self._encode.push(KEEP_ALIVE)
      self._keepAlive = 0
    } else {
      self._keepAlive++
    }
  }
}

Protocol.prototype.add = function (feed) {
  var self = this
  var keyHex = feed.publicId.toString('hex')
  var channel = this._channels[keyHex]

  if (!channel) channel = this._channels[keyHex] = new Channel(this, feed.publicId)
  if (channel.feed) return

  feed.open(function (err) {
    if (self._channels[keyHex] !== channel || channel.feed) return
    if (err) channel.close()
    else channel._open(feed)
  })
}

Protocol.prototype.remove = function (feed) {
  var keyHex = feed.publicId.toString('hex')
  if (!this._channels[keyHex]) return
  this._channels[keyHex].close()
}

Protocol.prototype.list = function () {
  var keys = Object.keys(this._channels)
  var list = []

  for (var i = 0; i < keys.length; i++) {
    var channel = this._channels[keys[i]]
    if (channel) list.push(channel)
  }

  return list
}

Protocol.prototype._parse = function (data) {
  this._remoteKeepAlive = 0
  if (!data.length) return

  var remote = varint.decode(data)
  var offset = varint.decode.bytes

  if (remote === this._remote.length) this._remote.push(null)
  if (remote > this._remote.length) {
    console.log('??', remote, data)
    return this.destroy(new Error('Received invalid channel'))
  }

  if (this._remote[remote]) {
    this._remote[remote]._parse(data, offset)
    return
  }

  if (this._remote.indexOf(null) !== remote) {
    return this.destroy(new Error('Received invalid channel'))
  }

  this._add(remote, data, offset)
}

Protocol.prototype._close = function () {
  var channels = this.list()
  for (var i = 0; i < channels.length; i++) {
    channels[i].close()
  }
}

Protocol.prototype._add = function (remote, data, offset) {
  try {
    var open = messages.Open.decode(data, offset)
  } catch (err) {
    return
  }

  if (open.publicId.length !== 32 || open.nonce.length !== 24) return

  var keyHex = open.publicId.toString('hex')
  var channel = this._channels[keyHex]

  if (channel) {
    channel._onopen(open, remote)
    return
  }

  channel = this._channels[keyHex] = new Channel(this, open.publicId)
  channel._onopen(open, remote)
  this.emit('add', open.publicId)
}

function noop () {}
