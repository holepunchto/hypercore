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

    var message = pickBlock(this)
    if (!message) return

    this.requests.push(message.block)
    this.feed.peersRequesting.set(message.block, true)
    this.channel.request(message)
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

  // meta events
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

  if (remove(peer.requests, message.block) === 0) this._tick = 0
  if (peer.responses.push(message) === 1) write(peer)

  peer.update()
}

function oncancel (message) {
  var peer = this.state

  for (var i = 0; i < peer.remoteRequests.length; i++) {
    var req = peer.remoteRequests[i]

    if (req.block === message.block && req.bytes === message.bytes) {
      if (i) peer.remoteRequests.splice(i, 1)
      else peer.remoteRequests.shift()
      return
    }
  }
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

  // timeout!

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
  for (var i = 0; i < self.feed.prioritized.length; i++) {
    var message = self.feed.prioritized[i]

    if (message.block === -1) {
      message.block = pickUnprioritizedBlock(self)
      if (message.block === -1) continue
    }

    message.nodes = self.feed.digest(message.block)
    if (validBlock(self, message.block)) return message
  }

  var block = pickUnprioritizedBlock(self)
  if (block === -1) return null

  return {
    block: block,
    nodes: self.feed.digest(block)
  }
}

function pickUnprioritizedBlock (self) {
  var i = 0
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
    if (err) return self.channel.destroy(err)

    self.feed.emit('download', next.block, next.value)
    self.responses.shift()
    if (self.responses.length) write(self)
  })
}

function readBytes (self) {
  var next = self.remoteRequests[0]

  self.feed._seek(next.bytes, function (err, index) {
    if (err || index & 1) { // Could not fulfil byte request
      next.bytes = 0
      read(self)
      return
    }

    var block = index / 2

    if (self.remoteBitfield.get(block)) {
      self.remoteRequests.shift()
      if (self.remoteRequests.length) read(self)
      return
    }

    // TODO: use nodes
    self.remoteRequests.unshift({
      block: block,
      bytes: 0,
      hash: self.hash
    })

    read(self)
  })
}

function read (self) { // TODO: use a remote tree as well
  var next = self.remoteRequests[0]

  while (self.remoteBitfield.get(next.block) && !next.bytes) {
    self.remoteRequests.shift()
    if (!self.remoteRequests.length) return
    next = self.remoteRequests[0]
  }

  var opts = {digest: next.nodes}
  if (next.bytes) return readBytes(self)

  self.feed.proof(next.block, opts, function (err, proof) {
    if (err) return self.channel.destroy(err)

    self.feed.get(next.block, function (err, data) {
      if (err) return self.channel.destroy(err)

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
