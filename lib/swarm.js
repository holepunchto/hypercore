var bitfield = require('./bitfield')
var flat = require('flat-tree')
var zlib = require('zlib')

module.exports = Swarm

function Swarm (feed) {
  if (!(this instanceof Swarm)) return new Swarm(feed)

  var self = this

  this.feed = feed
  this.channels = []
  this.selections = []

  this._blocks = 0
  this._critical = []
  this._requesting = bitfield(8)
  this._synchronized = 0
  this._synchronizedEmitted = 0

  feed.on('have', function (block, data) {
    for (var i = 0; i < self.channels.length; i++) {
      var channel = self.channels[i]
      if (!channel.remoteBitfield.get(block)) {
        channel.have({indexes: [2 * block]})
      }
    }
    feed.emit('download', block, data) // TODO: this is wrong as it'll get emitted on append
  })

  process.nextTick(function () {
    feed.open(function (err) {
      if (!err) self._maybeSynced()
    })
  })
}

Swarm.prototype._maybeSynced = function () {
  while (this.feed.bitfield.get(this._synchronized)) {
    this._synchronized++
  }

  if (this._synchronized === this.feed.blocks && this.feed.blocks) {
    if (this._synchronizedEmitted === this._synchronized) return
    this._synchronizedEmitted = this._synchronized
    this.feed.emit('synchronized')
  }
}

Swarm.prototype.select = function (from, to, priority) {
  throw new Error('Not yet implemented')
}

Swarm.prototype.deselect = function (from, to, priority) {
  throw new Error('Not yet implemented')
}

Swarm.prototype.critical = function (block, cb) {
  this._critical.push({block: block, callback: cb})
}

Swarm.prototype._checkCritical = function (block, data) {
  if (!this._critical.length) return

  var picked = null

  for (var i = 0; i < this._critical.length; i++) {
    if (this._critical[i].block === block) {
      if (!picked) picked = []
      picked.push(i)
    }
  }

  while (picked && picked.length) {
    var next = picked.pop()
    var pick = this._critical[next]
    this._critical.splice(pick, 1)
    pick.callback(null, data)
  }
}

Swarm.prototype.add = function (channel) {
  var self = this
  this.channels.push(channel)
  this.onchannel(channel)
  channel.once('close', function () {
    self.remove(channel)
  })
}

Swarm.prototype.remove = function (channel) {
  var i = this.channels.indexOf(channel)
  if (i > -1) this.channels.splice(i, 1)

  for (i = 0; i < channel.requests.length; i++) {
    this._requesting.set(channel.requests[i], false)
  }
  for (i = 0; i < this.channels.length; i++) {
    this.update(channel)
  }
}

Swarm.prototype.onchannel = function (channel) {
  var self = this
  var feed = this.feed

  var synchronized = 0
  var synchronizedEmitted = 0

  channel.requests = []
  channel.remoteBitfield = bitfield(16)
  channel.remotePausing = false

  channel.on('bitfield', onbitfield)
  channel.on('have', onhave)
  channel.on('resume', onresume)
  channel.on('pause', onpause)
  channel.on('response', onresponse)
  channel.on('request', onrequest)

  feedBitfield(feed, function (err, bitfield) {
    if (err) return channel.close()
    if (bitfield) channel.bitfield(bitfield)
    channel.resume()
  })

  function destroy () {
    channel.close()
  }

  function maybeSynchronized () {
    while (channel.remoteBitfield.get(synchronized)) {
      synchronized++
    }

    if (synchronized === feed.blocks && feed.blocks) {
      if (synchronizedEmitted === synchronized) return
      synchronizedEmitted = synchronized
      feed.emit('remote-synchronized')
    }
  }

  function onresume () {
    channel.remotePausing = false
    self.update(channel)
  }

  function onpause () {
    channel.remotePausing = true
  }

  function onresponse (response) {
    if (response.index & 1) return // only support reponses for a single block for now
    var block = response.index / 2

    feed.put(block, response.data, response, function (err) {
      if (err) return destroy(err)

      var i = channel.requests.indexOf(block)
      if (i > -1) channel.requests.splice(i, 1)

      self._checkCritical(block, response.data)
      self._maybeSynced()
      self.update(channel)
    })
  }

  function onrequest (request) {
    if (request.index & 1) return // only support requests for a single block for now
    var block = request.index / 2

    // TODO: pass in remoteTree
    channel.remoteBitfield.set(block, true)

    feed.proof(block, {digest: request.digest}, function (err, proof) {
      if (err) return destroy(err)
      feed.get(block, function (err, data) {
        if (err) return destroy(err)

        var response = {
          index: request.index,
          data: data,
          nodes: proof.nodes,
          signature: proof.signature
        }

        channel.response(response)
        feed.emit('upload', block, data)
      })
    })
  }

  function onbitfield (message) {
    decompressBitfield(message, function (err, buffer) {
      if (err) return channel.close(err)

      channel.remoteBitfield = bitfield(buffer)

      var end = Math.max(0, channel.remoteBitfield.length - 8)
      var max = 0

      for (var i = channel.remoteBitfield.length - 1; i >= end; i--) {
        if (channel.remoteBitfield.get(i)) {
          max = i + 1
          break
        }
      }

      if (self._blocks < max) self._blocks = max
      maybeSynchronized()
      self.update(channel)
    })
  }

  function onhave (have) {
    var max = 0

    for (var i = 0; i < have.indexes.length; i++) {
      var index = have.indexes[i]
      var start = flat.leftSpan(index) / 2
      var end = flat.rightSpan(index) / 2

      while (start <= end) {
        var block = start++
        if (block >= max) max = block + 1
        channel.remoteBitfield.set(block, true)
      }
    }

    if (self._blocks < max) self._blocks = max

    maybeSynchronized()
    self.update(channel)
  }
}

Swarm.prototype.pickBlock = function (channel) {
  var seen = 0
  var block = -1
  var i = 0

  for (i = 0; i < this._critical.length; i++) {
    var blk = this._critical[i].block
    if (!this._requesting.get(blk) && channel.remoteBitfield.get(blk)) {
      return blk
    }
  }

  for (i = 0; i < this._blocks; i++) {
    if (this.feed.bitfield.get(i) || !channel.remoteBitfield.get(i) || this._requesting.get(i)) continue
    if (Math.random() < (1 / ++seen)) block = i
  }

  return block
}

Swarm.prototype.update = function (channel) {
  if (channel.remotePausing) return

  var block = -1

  while (channel.requests.length < 16 && (block = this.pickBlock(channel)) > -1) {
    this._requesting.set(block, true)
    channel.requests.push(block)
    channel.request({
      index: 2 * block,
      digest: this.feed.digest(block)
    })
  }
}

function decompressBitfield (bitfield, cb) {
  if (!bitfield.compressed) return cb(null, bitfield.buffer)
  zlib.gunzip(bitfield.buffer, cb)
}

function feedBitfield (feed, cb) {
  var buffer = trim(feed.bitfield.buffer)
  if (!buffer.length) return cb(null, null)
  zlib.gzip(buffer, function (err, gzipped) {
    var compressed = !err && gzipped.length < buffer.length
    if (compressed) return cb(null, {buffer: gzipped, compressed: true})
    cb(null, {buffer: buffer})
  })
}

function trim (buf) {
  var length = buf.length
  while (length && !buf[length - 1]) length--
  if (length !== buf.length) return buf.slice(0, length)
  return buf
}
