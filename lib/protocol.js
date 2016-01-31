var lpstream = require('length-prefixed-stream')
var duplexify = require('duplexify')
var ids = require('numeric-id-map')
var crypto = require('crypto')
var events = require('events')
var bitfield = require('bitfield')
var util = require('util')
var messages = require('./messages')

var MAX_BITFIELD = 4 * 1024 * 1024
var MAX_MESSAGE = 5 * 1024 * 1024
var MAX_SINGLE_BYTE_VARINT = 127
var EXTENSION_OFFSET = 64
var MAX_EXTENSIONS = 64 // theoretically we can support any amount though

var ENCODINGS = [
  messages.Join,
  messages.Leave,
  messages.Have,
  messages.Pause,
  messages.Unpause,
  messages.Request,
  messages.Response,
  messages.Cancel
]

module.exports = Protocol

function Channel (stream, link) {
  events.EventEmitter.call(this)

  this.stream = stream
  this.left = false
  this.link = link
  this.local = 0
  this.remote = 0
  this.remoteJoined = false

  this.remotePausing = true
  this.remoteBitfield = bitfield(1, {grow: MAX_BITFIELD})
  this.amPausing = true
  this.amRequesting = bitfield(1, {grow: Infinity})
  this.inflight = 0
}

util.inherits(Channel, events.EventEmitter)

Channel.prototype.leave = function (err) {
  if (this.left) return
  if (err) this.emit('warn', err)
  this.stream._push(1, {channel: this.local})
  this.onleave()
}

Channel.prototype.bitfield = function (bitfield) {
  this.stream._push(2, {
    channel: this.local,
    bitfield: toBuffer(bitfield)
  })
}

Channel.prototype.have = function (blocks, bitfield) {
  if (typeof blocks === 'number') blocks = [blocks]
  this.stream._push(2, {
    channel: this.local,
    blocks: blocks,
    bitfield: toBuffer(bitfield)
  })
}

Channel.prototype.pause = function () {
  this.amPausing = true
  this.stream._push(3, {channel: this.local})
}

Channel.prototype.unpause = function () {
  this.amPausing = false
  this.stream._push(4, {channel: this.local})
}

Channel.prototype.request = function (block) {
  this.amRequesting.set(block, true)
  this.inflight++
  this.stream.inflight++
  this.stream._push(5, {channel: this.local, block: block})
}

Channel.prototype.response = function (block, data, proof) {
  this.stream._push(6, {
    channel: this.local,
    block: block,
    data: data,
    proof: proof
  })
}

Channel.prototype.cancel = function (block) {
  this._clear(block)
  this.stream._push(7, {
    channel: this.local,
    block: block
  })
}

Channel.prototype.onleave = function () {
  if (this.left) return
  this.left = true
  if (this.remoteJoined) this.stream._remove(this)
  this.emit('leave')
}

Channel.prototype.onjoin = function (channel) {
  this.remote = channel
  this.remoteJoined = true
  if (this.left) this.stream._remove(this)
}

Channel.prototype.onhave = function (message) {
  if (message.bitfield) {
    this.remoteBitfield = bitfield(message.bitfield, {grow: MAX_BITFIELD})
  }
  if (message.blocks) {
    for (var i = 0; i < message.blocks.length; i++) {
      var block = message.blocks[i]
      this.remoteBitfield.set(block)
    }
  }

  this.emit('have')
  this.emit('update')
}

Channel.prototype.onpause = function () {
  this.remotePausing = true
  this.emit('pause')
  this.emit('update')
}

Channel.prototype.onunpause = function () {
  this.remotePausing = false
  this.emit('unpause')
  this.emit('update')
}

Channel.prototype.onrequest = function (message) {
  this.emit('request', message.block)
}

Channel.prototype.onresponse = function (message) {
  var block = message.block
  this._clear(block)
  this.emit('response', block, message.data, message.proof)
}

Channel.prototype.oncancel = function (message) {
  this.emit('cancel', message.block)
}

Channel.prototype._clear = function (block) {
  if (this.amRequesting.get(block)) {
    this.inflight--
    this.stream.inflight--
    this.amRequesting.set(block, false)
  }
}

function Protocol (opts) {
  if (!(this instanceof Protocol)) return new Protocol(opts)
  if (!opts) opts = {}

  duplexify.call(this)

  var self = this

  this.inflight = 0
  this.id = opts.id || crypto.randomBytes(32)
  this.remoteId = null

  this._parser = lpstream.decode({limit: MAX_MESSAGE})
  this._generator = lpstream.encode()
  this._parser.on('data', parse)
  this._handshook = false

  this._channels = {}
  this._remote = []
  this._local = ids()

  this._extensions = opts.extensions || []
  if (this._extensions.length > MAX_EXTENSIONS) throw new Error('Only ' + MAX_EXTENSIONS + ' are supported')
  this._remoteExtensions = new Array(this._extensions.length)
  this._localExtensions = new Array(this._extensions.length)
  for (var i = 0; i < this._extensions.length; i++) {
    this._remoteExtensions[i] = this._localExtensions[i] = -1
  }

  var handshake = {
    protocol: 'hypercore',
    id: this.id,
    extensions: opts.extensions
  }

  this._generator.write(messages.Handshake.encode(handshake))

  this.setReadable(this._generator)
  this.setWritable(this._parser)
  this.on('close', onclose)

  function onclose () {
    for (var i = 0; i < this._local.length; i++) {
      var ch = this._local.get(i)
      if (ch) ch.onleave()
    }
  }

  function parse (data) {
    self._parse(data)
  }
}

util.inherits(Protocol, duplexify)

Protocol.prototype.remoteSupports = function (id) {
  var i = typeof id === 'number' ? id : this._extensions.indexOf(id)
  return this._localExtensions[i] > -1
}

Protocol.prototype.send = function (id, buf) {
  var i = typeof id === 'number' ? id : this._extensions.indexOf(id)

  if (!this.remoteSupports(i)) return false
  if (typeof buf === 'string') buf = new Buffer(buf)

  var msg = new Buffer(buf.length + 1)
  msg[0] = i + EXTENSION_OFFSET
  buf.copy(msg, 1)
  this._generator.write(msg)

  return true
}

Protocol.prototype._onhandshake = function (handshake) {
  if (handshake.protocol !== 'hyperdrive' && handshake.protocol !== 'hypercore') {
    return this.destroy(new Error('Protocol must be hypercore'))
  }

  // extensions *must* be sorted
  var local = 0
  var remote = 0

  while (local < this._extensions.length && remote < handshake.extensions.length && remote < MAX_EXTENSIONS) {
    if (this._extensions[local] === handshake.extensions[remote]) {
      this._localExtensions[local] = remote
      this._remoteExtensions[remote] = local
      local++
      remote++
    } else if (this._extensions[local] < handshake.extensions[remote]) {
      local++
    } else {
      remote++
    }
  }

  this.remoteId = handshake.id
  this.emit('handshake')
}

Protocol.prototype._parse = function (data) {
  if (this.destroyed) return

  if (!this._handshook) {
    try {
      var handshake = messages.Handshake.decode(data)
    } catch (err) {
      return this.destroy(err)
    }
    this._handshook = true
    this._onhandshake(handshake)
    return
  }

  var type = data[0]
  if (type > MAX_SINGLE_BYTE_VARINT) return
  if (type >= EXTENSION_OFFSET) return this._onextension(type, data)
  if (type >= ENCODINGS.length) return // unknown message

  try {
    var message = ENCODINGS[type].decode(data, 1)
  } catch (err) {
    return this.destroy(err)
  }

  switch (type) {
    case 0: return this._onjoin(message)
    case 1: return this._onleave(message)
    case 2: return this._onhave(message)
    case 3: return this._onpause(message)
    case 4: return this._onunpause(message)
    case 5: return this._onrequest(message)
    case 6: return this._onresponse(message)
  }
}

Protocol.prototype._onextension = function (type, message) {
  var ext = this._remoteExtensions[type - EXTENSION_OFFSET]
  if (ext > -1) this.emit(this._extensions[ext], message.slice(1))
}

Protocol.prototype._onjoin = function (message) {
  if (message.channel > this._remote.length) {
    return this.destroy(new Error('Remote sent invalid channel id'))
  }

  if (this._remote.length > message.channel && this._remote[message.channel]) {
    return this.destroy(new Error('Remote reused channel id'))
  }

  var ch = this.join(message.id)
  if (this._remote.length === message.channel) this._remote.push(null)
  this._remote[message.channel] = ch
  ch.onjoin(message.channel)
}

Protocol.prototype._onleave = function (message) {
  var ch = this._get(message.channel)
  if (ch) ch.onleave()
}

Protocol.prototype._onhave = function (message) {
  var ch = this._get(message.channel)
  if (ch) ch.onhave(message)
}

Protocol.prototype._onpause = function (message) {
  var ch = this._get(message.channel)
  if (ch && !ch.remotePausing) ch.onpause()
}

Protocol.prototype._onunpause = function (message) {
  var ch = this._get(message.channel)
  if (ch && ch.remotePausing) ch.onunpause()
}

Protocol.prototype._onrequest = function (message) {
  var ch = this._get(message.channel)
  if (ch) ch.onrequest(message)
}

Protocol.prototype._onresponse = function (message) {
  var ch = this._get(message.channel)
  if (ch) ch.onresponse(message)
}

Protocol.prototype._get = function (id) {
  if (id >= this._remote.length) {
    this.destroy(new Error('Remote sent invalid channel id'))
    return null
  }
  return this._remote[id]
}

Protocol.prototype._remove = function (channel) {
  this._local.remove(channel.local)
  this._remote[channel.remote] = null
  delete this._channels[channel.link.toString('hex')]
}

Protocol.prototype._push = function (type, message) {
  var enc = ENCODINGS[type]
  var buf = new Buffer(enc.encodingLength(message) + 1)
  buf[0] = type
  enc.encode(message, buf, 1)
  this._generator.write(buf)
}

Protocol.prototype.join = function (id) {
  var name = id.toString('hex')
  var ch = this._channels[name]
  if (ch) return this._local.get(ch - 1)

  ch = new Channel(this, id)
  ch.local = this._local.add(ch)
  this._channels[name] = ch.local + 1
  this._push(0, {channel: ch.local, id: id})
  this.emit('channel', ch)

  return ch
}

Protocol.prototype.leave = function (id) {
  var name = id.toString('hex')
  if (!this._channels[name]) return false
  this.join(id).leave()
}

function toBuffer (bitfield) {
  if (!bitfield) return null
  if (Buffer.isBuffer(bitfield)) return bitfield
  return bitfield.buffer
}
