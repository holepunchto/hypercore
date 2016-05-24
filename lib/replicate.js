var protocol = require('hypercore-protocol')
var bitfield = require('./bitfield')
var rle = require('bitfield-rle')
var unordered = require('unordered-set')

module.exports = replicate

function replicate (core, feed, opts) {
  if (!opts) opts = {}

  var stream = opts.stream

  if (!stream) {
    stream = protocol({id: core.id, private: opts.private !== false})
    stream.setTimeout(opts.timeout || 5000, stream.destroy)
  }

  if (feed) {
    feed.open(function (err) {
      if (err) return stream.destroy(err)
      ready(stream, feed)
    })
  }

  return stream
}

function Peer (feed) {
  this._tick = 0
  this._index = -1

  this.maxRequests = 16 // arbitrary for now. should be more flex in the future
  this.feed = feed
  this.channel = null

  this.remotePausing = false
  this.remoteBitfield = bitfield(16)
  this.remoteRequests = []

  this.pausing = false
  this.responses = []
  this.requests = []
}

// called by the feed when it has a new block

Peer.prototype.have = function (block) {
  if (this.channel.opened) this.channel.have({start: block})
}

// expose the update methods for easier debugging

Peer.prototype.update = function () {
  while (true) {
    if (this.requests.length >= this.maxRequests) return

    var block = pickBlock(this)
    if (block === -1) return

    this.requests.push(block)
    this.feed.peersRequesting.set(block, true)
    this.channel.request({
      block: block,
      nodes: this.feed.digest(block)
    })
  }
}

Peer.prototype.updateAll = function () {
  for (var i = 0; i < this.feed.peers.length; i++) {
    this.feed.peers[i].update()
  }
}

function ready (stream, feed) {
  var peer = new Peer(feed)
  var channel = peer.channel = stream.open(feed.key, {state: peer})

  unordered.add(feed.peers, peer)

  channel.on('have', onhave)
  channel.on('want', onwant)
  channel.on('request', onrequest)
  channel.on('data', ondata)
  channel.on('cancel', oncancel)
  channel.on('pause', onpause)
  channel.on('resume', onresume)

  channel.on('tick', ontick)
  channel.on('end', onend)

  if (channel.opened) onopen.call(channel)
  else channel.on('open', onopen)
}

function onopen () {
  var peer = this.state

  peer.channel.have({
    start: 0,
    bitfield: rle.encode(peer.feed.bitfield.buffer)
  })
}

function onwant (message) {
  var peer = this.state
  var byteOffset = Math.floor(message.start / 8)

  peer.channel.have({
    start: byteOffset * 8,
    bitfield: rle.encode(this.feed.bitfield.buffer.slice(byteOffset))
  })
}

function onhave (message) {
  var peer = this.state

  // TODO: we should protect better / make configurable how big the BF grows

  var start = message.start
  var end = message.end || (start + 1)

  if (message.bitfield) {
    var bits = bitfield(rle.decode(message.bitfield))
    for (var i = 0; i < bits.length; i++) {
      peer.remoteBitfield.set(start + i, bits.get(i))
    }
  } else {
    while (start < end) {
      peer.remoteBitfield.set(start++, true)
    }
  }

  peer.update() // TODO: if previous update failed we should only look in the update state
}

function onrequest (message) {
  var peer = this.state

  if (peer.remoteRequests.push(message) === 1) read(peer)
}

function ondata (message) {
  var peer = this.state

  if (!remove(peer.requests, message.block)) this._tick = 0
  if (peer.responses.push(message) === 1) write(peer)

  peer.update()
}

function oncancel (message) {
  var peer = this.state

  remove(peer.remoteRequests, message.block)
}

function onpause (message) {
  var peer = this.state

  peer.remotePausing = true
  while (peer.requests.length) {
    peer.feed.peersRequesting.set(peer.requests.shift(), false)
  }
  peer._tick = 0
  peer.updateAll()
  peer.emit('pause')
}

function onresume (message) {
  var peer = this.state

  peer.remotePausing = false
  peer.update()
}

function ontick () {
  var peer = this.state

  if (!peer.requests.length || ++peer._tick < 4) return
  peer.feed.peersRequesting.set(peer.requests.shift(), false)
  peer.updateAll()
}

function onend () {
  var peer = this.state

  while (peer.requests.length) {
    peer.feed.peersRequesting.set(peer.requests.shift(), false)
  }

  unordered.remove(peer.feed.peers, peer)
  peer.updateAll()
  peer.emit('close')
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

  self.feed.put(next.block, next.value, next, function (err) {
    if (err) return self.feed.destroy(err)

    self.feed.emit('download', next.block, next.value)
    self.responses.shift()
    if (self.responses.length) write(self)
  })
}

function read (self) { // TODO: use a remote tree as well
  var next = self.remoteRequests[0]
  var opts = {digest: next.nodes}

  self.feed.proof(next.block, opts, function (err, proof) {
    if (err) return self.feed.destroy(err)

    self.feed.get(next.block, function (err, data) {
      if (err) return self.feed.destroy(err)

      self.feed.emit('upload', next.block, data)
      self.remoteBitfield.set(next.block, true)
      self.channel.data({
        block: next.block,
        value: data,
        nodes: proof.nodes,
        signature: proof.signature
      })

      self.remoteRequests.shift()
      if (self.remoteRequests.length) read(self)
    })
  })
}
