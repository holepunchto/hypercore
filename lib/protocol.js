var messages = require('./messages')
var hash = require('./hash')
var stream = require('stream')
var sodium = require('sodium-native')
var varint = require('varint')
var inherits = require('inherits')

module.exports = Protocol

function Protocol (opts) {
  if (!(this instanceof Protocol)) return new Protocol(opts)
  if (Buffer.isBuffer(opts)) opts = {key: opts}
  if (!opts) opts = {}

  stream.Duplex.call(this)

  this.id = null
  this.remoteId = null

  this.key = null
  this.discoveryKey = null
  this.nonce = null
  this.remoteNonce = null
  this.remoteDiscoveryKey = null
  this.encrypted = opts.encrypt !== false
  this.destroyed = false
  this.state = opts.state

  // parsing handlers
  this.onopen = noop
  this.onhandshake = noop
  this.onactive = noop
  this.oninactive = noop
  this.onhave = noop
  this.onunhave = noop
  this.onwant = noop
  this.onunwant = noop
  this.onrequest = noop
  this.oncancel = noop
  this.ondata = noop
  this.onclose = noop
  this.ontimeout = noop
  this.ontick = noop

  this._xor = null
  this._remoteXor = null
  this._message = null
  this._length = new Buffer(varint.encodingLength(8388608))
  this._ptr = 0
  this._missing = 0
  this._overflow = null
  this._done = null
  this._interval = null
  this._keepAlive = 0
  this._remoteKeepAlive = 0

  this.on('finish', function () {
    this.finalize()
  })

  if (opts.key) this.ready(opts.key)
}

inherits(Protocol, stream.Duplex)

Protocol.prototype.finalize = function () {
  this.push(null)
  this.destroy()
}

// Protocol.prototype.open = function (msg) {
//   return this._encryptAndPush(messages.Open, 0, msg)
// }

Protocol.prototype.handshake = function (msg) {
  return this._encryptAndPush(messages.Handshake, 0, msg)
}

Protocol.prototype.active = function (msg) {
  return this._encryptAndPush(messages.Active, 1, msg)
}

Protocol.prototype.inactive = function (msg) {
  return this._encryptAndPush(messages.Inactive, 2, msg)
}

Protocol.prototype.have = function (msg) {
  return this._encryptAndPush(messages.Have, 3, msg)
}

Protocol.prototype.unhave = function (msg) {
  return this._encryptAndPush(messages.Unhave, 4, msg)
}

Protocol.prototype.want = function (msg) {
  return this._encryptAndPush(messages.Want, 5, msg)
}

Protocol.prototype.unwant = function (msg) {
  return this._encryptAndPush(messages.Unwant, 6, msg)
}

Protocol.prototype.request = function (msg) {
  return this._encryptAndPush(messages.Request, 7, msg)
}

Protocol.prototype.cancel = function (msg) {
  return this._encryptAndPush(messages.Cancel, 8, msg)
}

Protocol.prototype.data = function (msg) {
  return this._encryptAndPush(messages.Data, 9, msg)
}

Protocol.prototype.close = function (msg) {
  return this._encryptAndPush(messages.Close, 10, msg)
}

Protocol.prototype.setTimeout = function (ms) {
  if (this.destroyed) return

  var self = this

  this._keepAlive = 0
  this._remoteKeepAlive = 0

  clearInterval(this._interval)
  if (!ms) return

  this._interval = setInterval(kick, (ms / 4) | 0)
  if (this._interval.unref) this._interval.unref()

  function kick () {
    self._kick()
  }
}

Protocol.prototype._kick = function () {
  if (this._remoteKeepAlive > 4) {
    clearInterval(this._interval)
    this.ontimeout(this.state)
    return
  }

  this.ontick(this.state)
  this._remoteKeepAlive++

  if (this._keepAlive > 2) {
    if (this.key) this.ping()
    this._keepAlive = 0
  } else {
    this._keepAlive++
  }
}

Protocol.prototype.ping = function () {
  var ping = new Buffer([0])
  if (this._xor) this._xor.update(ping, ping)
  return this.push(ping)
}

Protocol.prototype._encryptAndPush = function (enc, type, msg) {
  if (!this.key) throw new Error('You have to call .ready first')
  this._keepAlive = 0
  var buf = encode(enc, type, msg)
  if (this._xor) this._xor.update(buf, buf)
  return this.push(buf)
}

Protocol.prototype.ready = function (key) {
  if (this.destroyed) return

  this.key = key
  this.nonce = hash.randomBytes(24)
  this.discoveryKey = hash.discoveryKey(key)

  var open = messages.Open.encode({discoveryKey: this.discoveryKey, nonce: this.nonce})

  this.push(Buffer.concat([new Buffer([open.length + 1, 0]), open]))

  if (this.encrypted) {
    this._xor = sodium.crypto_stream_xor_instance(this.nonce, this.key)
    this._ready()
  }
}

Protocol.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  clearInterval(this._interval)
  if (err) this.emit('error', err)
  this.emit('close')
}

Protocol.prototype._write = function (data, enc, cb) {
  this._remoteKeepAlive = 0
  this._descryptAndParse(data, cb)
}

Protocol.prototype._descryptAndParse = function (data, cb) {
  if (this._remoteXor) this._remoteXor.update(data, data)
  this._parse(data, 0, cb)
}

Protocol.prototype._onmessage = function (data, start, end) {
  if (!this.remoteDiscoveryKey) {
    var open = decode(messages.Open, data, start + 1, end)
    if (!open) return this.destroy(new Error('Invalid open message'))

    this.remoteNonce = open.nonce
    this.remoteDiscoveryKey = open.discoveryKey // TODO: validate

    this.onopen(open)
    this._ready()
    return
  }
  var channel = data[start++]

  if (channel !== 0) throw new Error('Only channel 0 is supported')

  var type = data[start++]

  switch (type) {
    case 0: return this._emit(this.onhandshake, decode(messages.Handshake, data, start, end))
    case 1: return this._emit(this.onactive, decode(messages.Active, data, start, end))
    case 2: return this._emit(this.oninactive, decode(messages.Inactive, data, start, end))
    case 3: return this._emit(this.onhave, decode(messages.Have, data, start, end))
    case 4: return this._emit(this.onunhave, decode(messages.Unhave, data, start, end))
    case 5: return this._emit(this.onwant, decode(messages.Want, data, start, end))
    case 6: return this._emit(this.onunwant, decode(messages.Unwant, data, start, end))
    case 7: return this._emit(this.onrequest, decode(messages.Request, data, start, end))
    case 8: return this._emit(this.oncancel, decode(messages.Cancel, data, start, end))
    case 9: return this._emit(this.ondata, decode(messages.Data, data, start, end))
    case 10: return this._emit(this.onclose, decode(messages.Close, data, start, end))
  }
}

Protocol.prototype._emit = function (fn, msg) {
  if (msg) fn(msg, this.state)
}

Protocol.prototype._parse = function (data, start, cb) {
  while (start < data.length && !this.destroyed) {
    while (!this._missing && start < data.length) {
      var byte = this._length[this._ptr++] = data[start++]

      if (!(byte & 0x80)) {
        this._missing = varint.decode(this._length)
        this._ptr = 0
        if (this._missing > 8388608) return this._tooBig()
        break
      }

      if (this._ptr >= this._length.length) return this._tooBig()
    }

    if (!this._missing) continue

    var free = data.length - start
    var missing = this._missing

    if (free >= missing) {
      this._missing = 0

      var first = !this.remoteDiscoveryKey

      if (this._message) {
        data.copy(this._message, this._ptr, start)
        this._onmessage(this._message, 0, this._message.length)
        this._message = null
        this._ptr = 0
      } else {
        this._onmessage(data, start, start + missing)
      }

      start += missing

      if (first && this.key) {
        this._write(data.slice(start), null, cb)
        return
      }

    } else {
      if (!this._message) this._message = new Buffer(missing)
      data.copy(this._message, this._ptr, start)
      this._missing -= free
      this._ptr += free
      start += free
    }

    if (!this.key && this.remoteDiscoveryKey && this.encrypted) {
      this._overflow = data.slice(start)
      this._done = cb
      return
    }
  }

  cb()
}

Protocol.prototype._ready = function () {
  if (this.key && this.remoteDiscoveryKey && this.encrypted && !this._remoteXor) {
    this._remoteXor = sodium.crypto_stream_xor_instance(this.remoteNonce, this.key)

    var cb = this._done
    var overflow = this._overflow

    this._done = null
    this._overflow = null

    if (overflow) this._write(overflow, null, cb)
  }
}

Protocol.prototype._read = function () {
  // do nothing
}

Protocol.prototype._tooBig = function () {
  this.destroy(new Error('Incoming message is > 8MB'))
}

function noop () {}

function decode (enc, buf, start, end) {
  try {
    return enc.decode(buf, start, end)
  } catch (err) {
    return null
  }
}

function encode (enc, type, msg) {
  var channel = 0
  var len = enc.encodingLength(msg) + 1 + 1
  var buf = new Buffer(len + varint.encodingLength(len))
  var offset = 0

  varint.encode(len, buf, offset)
  offset += varint.encode.bytes
  buf[offset++] = channel
  buf[offset++] = type
  enc.encode(msg, buf, offset)

  return buf
}
