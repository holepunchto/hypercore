var protocol = require('./protocol')
var bitfield = require('./bitfield')

module.exports = Swarm

function Swarm (feed) {
  if (!(this instanceof Swarm)) return new Swarm(feed)
  this.feed = feed
  this.wires = []
  this.selections = []

  this._blocks = 0
  this._critical = []
  this._requesting = bitfield(8)
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

Swarm.prototype.replicate = function (opts) {
  var self = this
  var stream = protocol(opts)

  stream.setTimeout(5000, stream.destroy)
  stream.on('channel', function (wire) {
    self.add(wire)
  })
  stream.join(this.feed)

  return stream
}

Swarm.prototype.add = function (wire) {
  var self = this
  this.wires.push(wire)
  this.onwire(wire)
  wire.once('close', function () {
    self.remove(wire)
  })
}

Swarm.prototype.remove = function (wire) {
  var i = this.wires.indexOf(wire)
  if (i > -1) this.wires.splice(i, 1)

  for (i = 0; i < wire.requests.length; i++) {
    this._requesting.set(wire.requests[i])
  }
  for (i = 0; i < this.wires.length; i++) {
    this.update(wire)
  }
}

Swarm.prototype.onwire = function (wire) {
  var self = this
  var feed = this.feed

  wire.requests = []
  wire.remoteBitfield = bitfield(16)
  wire.remotePausing = false

  feed.on('have', function (block) {
    if (!wire.remoteBitfield.get(block)) {
      wire.have({blocks: [block]})
    }
  })

  wire.on('have', function (have) {
    var i = 0
    var blocks = 0

    if (have.bitfield) {
      wire.remoteBitfield = bitfield(have.bitfield)
      for (i = Math.max(0, wire.remoteBitfield.length - 8); i < wire.remoteBitfield.length; i++) {
        if (wire.remoteBitfield.get(i)) blocks = i + 1
      }
    }

    if (have.blocks) {
      for (i = 0; i < have.blocks.length; i++) {
        wire.remoteBitfield.set(have.blocks[i], true)
        if (have.blocks[i] >= blocks) blocks = have.blocks[i] + 1
      }
    }

    if (self._blocks < blocks) {
      self._blocks = blocks
    }

    self.update(wire)
  })

  wire.on('resume', function () {
    wire.remotePausing = false
    self.update(wire)
  })

  wire.on('pause', function () {
    wire.remotePausing = true
  })

  wire.on('response', function (response) {
    feed.put(response.block, response.data, response, function (err) {
      if (err) return destroy(err)

      var i = wire.requests.indexOf(response.block)
      if (i > -1) wire.requests.splice(i, 1)

      feed.emit('download', response.block, response.data, response)
      self._checkCritical(response.block, response.data)
      self.update(wire)
    })
  })

  wire.on('request', function (request) {
    // TODO: pass in remoteTree
    wire.remoteBitfield.set(request.block, true)
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

        wire.response(response)
        feed.emit('upload', request.block, data, response)
      })
    })
  })

  wire.have({
    bitfield: trim(feed.bitfield.buffer)
  })

  wire.resume()

  function destroy () {
    wire.close()
  }
}

Swarm.prototype.pickBlock = function (wire) {
  var seen = 0
  var block = -1
  var i = 0

  for (i = 0; i < this._critical.length; i++) {
    var blk = this._critical[i].block
    if (!this._requesting.get(blk) && wire.remoteBitfield.get(blk)) {
      return blk
    }
  }

  for (i = 0; i < this._blocks; i++) {
    if (this.feed.bitfield.get(i) || !wire.remoteBitfield.get(i) || this._requesting.get(i)) continue
    if (Math.random() < (1 / ++seen)) block = i
  }

  return block
}

Swarm.prototype.update = function (wire) {
  if (wire.remotePausing) return

  var block = -1

  while (wire.requests.length < 16 && (block = this.pickBlock(wire)) > -1) {
    this._requesting.set(block, true)
    wire.requests.push(block)
    wire.request({
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
