var bitfield = require('./bitfield')
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
        channel.have({indexes: [block]})
      }
    }
    feed.emit('download', block, data)
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
    this._requesting.set(channel.requests[i])
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

  channel.on('have', onhave)

  channel.on('resume', function () {
    channel.remotePausing = false
    self.update(channel)
  })

  channel.on('pause', function () {
    channel.remotePausing = true
  })

  channel.on('response', function (response) {
    feed.put(response.block, response.data, response, function (err) {
      if (err) return destroy(err)

      var i = channel.requests.indexOf(response.block)
      if (i > -1) channel.requests.splice(i, 1)

      self._checkCritical(response.block, response.data)
      self._maybeSynced()
      self.update(channel)
    })
  })

  channel.on('request', function (request) {
    // TODO: pass in remoteTree
    channel.remoteBitfield.set(request.block, true)
    feed.proof(request.block, {digest: request.digest}, function (err, proof) {
      if (err) return destroy(err)
      feed.get(request.block, function (err, data) {
        if (err) return destroy(err)

        var response = {
          block: request.block,
          data: data,
          nodes: proof.nodes,
          signature: proof.signature
        }

        channel.response(response)
        feed.emit('upload', request.block, data)
      })
    })
  })

  var trimmed = trim(feed.bitfield.buffer)
  zlib.gzip(trimmed, function (err, gzipped) {
    if (!err && gzipped.length < trimmed.length) {
      channel.have({
        bitfield: gzipped,
        compressed: true
      })
    } else if (trimmed.length) {
      channel.have({
        bitfield: trimmed
      })
    }

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

  function oncompressedhave (have) {
    if (!have.bitfield) return
    have.compressed = false
    zlib.gunzip(have.bitfield, function (err, buffer) {
      if (err) return
      have.bitfield = buffer
      onhave(have)
    })
  }

  function onhave (have) {
    if (have.compressed) return oncompressedhave(have)

    var i = 0
    var blocks = 0

    if (have.bitfield) {
      channel.remoteBitfield = bitfield(have.bitfield)
      for (i = Math.max(0, channel.remoteBitfield.length - 8); i < channel.remoteBitfield.length; i++) {
        if (channel.remoteBitfield.get(i)) blocks = i + 1
      }
    }

    if (have.indexes) {
      for (i = 0; i < have.indexes.length; i++) {
        channel.remoteBitfield.set(have.indexes[i], true)
        if (have.indexes[i] >= blocks) blocks = have.indexes[i] + 1
      }
    }

    if (self._blocks < blocks) {
      self._blocks = blocks
    }

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
      block: block,
      digest: this.feed.digest(block)
    })
  }
}

function trim (buf) {
  var length = buf.length
  while (length && !buf[length - 1]) length--
  if (length !== buf.length) return buf.slice(0, length)
  return buf
}
