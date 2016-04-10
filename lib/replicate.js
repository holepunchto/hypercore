var bitfield = require('./bitfield')
var rle = require('bitfield-rle')
var inherits = require('inherits')
var events = require('events')
var unordered = require('unordered-set')
var protocol = require('./protocol')

module.exports = function (core, opts) {
  if (!opts) opts = {}
  if (!opts.id) opts.id = core.id
  if (!opts.extensions) opts.extensions = core.extensions

  var stream = protocol(opts, Peer)
  stream.setTimeout(opts.timeout || 5000, stream.destroy)
  if (opts.feeds) {
    for (var i = 0; i < opts.feeds.length; i++) stream.add(opts.feeds[i])
  }
  return stream
}

function Peer (stream, channel, feed) {
  events.EventEmitter.call(this)

  this._tick = 0
  this._index = -1

  this.maxRequests = 16 // arbitrary for now. should be more flex in the future
  this.feed = feed
  this.channel = channel

  this.remotePausing = false
  this.remoteBitfield = bitfield(16)
  this.remoteRequests = []

  this.pausing = false
  this.responses = []
  this.requests = []
  this.stream = stream

  this.ready()
}

inherits(Peer, events.EventEmitter)

Peer.prototype.have = function (message) {
  this.stream.send(this.channel, 1, message)
}

Peer.prototype.want = function (message) {
  this.stream.send(this.channel, 2, message)
}

Peer.prototype.request = function (message) {
  this.requests.push(message.block)
  this.feed.peersRequesting.set(message.block, true)
  this.stream.send(this.channel, 3, message)
}

Peer.prototype.response = function (message) {
  this.stream.send(this.channel, 4, message)
}

Peer.prototype.cancel = function (message) {
  if (!remove(this.requests, message.block)) this._tick = 0
  this.stream.send(this.channel, 5, message)
}

Peer.prototype.pause = function () {
  if (this.pausing) return
  this.pausing = true
  this.stream.send(this.channel, 6, null)
}

Peer.prototype.resume = function () {
  if (!this.pausing) return
  this.pausing = false
  this.stream.send(this.channel, 7, null)
}

Peer.prototype.ontick = function () { // TODO: impl me
  if (!this.requests.length || ++this._tick < 4) return
  this.feed.peersRequesting.set(this.requests.shift(), false)
  this.updateAll()
}

Peer.prototype.onclose = function () {
  while (this.requests.length) {
    this.feed.peersRequesting.set(this.requests.shift(), false)
  }

  unordered.remove(this.feed.peers, this)
  this.updateAll()
  this.emit('close')
}

Peer.prototype.onfeedhave = function (block) {
  this.have({start: block})
}

Peer.prototype.onmessage = function (type, message) {
  switch (type) {
    case 1: return this.onhave(message)
    case 2: return this.onwant(message)
    case 3: return this.onrequest(message)
    case 4: return this.onresponse(message)
    case 5: return this.oncancel(message)
    case 6: return this.onpause()
    case 7: return this.onresume()
  }
}

Peer.prototype.onhave = function (message) {
  // TODO: we should protect better / make configurable how big the BF grows

  var start = message.start
  var end = message.end || (start + 1)

  if (message.bitfield) {
    var bits = bitfield(rle.decode(message.bitfield))
    for (var i = 0; i < bits.length; i++) {
      this.remoteBitfield.set(start + i, bits.get(i))
    }
  } else {
    while (start < end) {
      this.remoteBitfield.set(start++, true)
    }
  }

  this.update() // TODO: if previous update failed we should only look in the update state
}

Peer.prototype.onwant = function (message) {
  var byteOffset = Math.floor(message.start / 8)

  this.have({
    start: byteOffset * 8,
    bitfield: rle.encode(this.feed.bitfield.buffer.slice(byteOffset))
  })
}

Peer.prototype.onrequest = function (message) {
  if (this.remoteRequests.push(message) === 1) read(this)
}

Peer.prototype.onresponse = function (message) {
  if (!remove(this.requests, message.block)) this._tick = 0
  if (this.responses.push(message) === 1) write(this)
  this.update()
}

Peer.prototype.oncancel = function (message) {
  remove(this.remoteRequests, message.block)
}

Peer.prototype.onpause = function () {
  this.remotePausing = true
  while (this.requests.length) {
    this.feed.peersRequesting.set(this.requests.shift(), false)
  }
  this._tick = 0
  this.updateAll()
  this.emit('pause')
}

Peer.prototype.onresume = function () {
  this.remotePausing = false
  this.update()
  this.emit('resume')
}

Peer.prototype.ready = function () {
  unordered.add(this.feed.peers, this)
  this.feed.emit('peer', this)

  this.have({
    start: 0,
    bitfield: rle.encode(this.feed.bitfield.buffer)
  })
}

Peer.prototype.update = function () {
  while (true) {
    if (this.requests.length >= this.maxRequests) return

    var block = pickBlock(this)
    if (block === -1) return

    this.request({
      block: block,
      digest: this.feed.digest(block)
    })
  }
}

Peer.prototype.updateAll = function () {
  for (var i = 0; i < this.feed.peers.length; i++) {
    this.feed.peers[i].update()
  }
}

function pickBlock (self) {
  var i = 0

  for (i = 0; i < self.feed.prioritized.length; i++) {
    var j = self.feed.prioritized[j]
    if (validBlock(self, j)) return j
  }

  var offset = Math.floor(Math.random() * self.remoteBitfield.length)

  for (i = offset; i < self.remoteBitfield.length; i++) {
    if (validBlock(self, i)) return i
  }
  for (i = 0; i < offset; i++) {
    if (validBlock(self, i)) return i
  }

  return -1
}

function validBlock (self, i) {
  return self.remoteBitfield.get(i) && !self.feed.bitfield.get(i) && !self.feed.peersRequesting.get(i)
}

function remove (list, val) {
  var i = list.indexOf(val)
  if (i === 0) list.shift()
  else if (i > -1) list.splice(i, 1)
  return i
}

function write (self) {
  var next = self.responses[0]

  self.feed.put(next.block, next.data, next, function (err) {
    if (err) return self.stream.destroy(err)

    self.feed.emit('download', next.block, next.data)
    self.responses.shift()
    if (self.responses.length) write(self)
  })
}

function read (self) { // TODO: use a remote tree as well
  var next = self.remoteRequests[0]
  var opts = {digest: next.digest}

  self.feed.proof(next.block, opts, function (err, proof) {
    if (err) return self.stream.destroy(err)

    self.feed.get(next.block, function (err, data) {
      if (err) return self.stream.destroy(err)

      self.feed.emit('upload', next.block, data)
      self.remoteBitfield.set(next.block, true)
      self.response({
        block: next.block,
        data: data,
        nodes: proof.nodes,
        signature: proof.signature
      })

      self.remoteRequests.shift()
      if (self.remoteRequests.length) read(self)
    })
  })
}
