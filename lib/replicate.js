var protocol = require('hypercore-protocol')
var bitfield = require('./bitfield')
var rle = require('bitfield-rle')
var unordered = require('unordered-set')
var inherits = require('inherits')
var events = require('events')
var tree = require('./tree-index')

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
      ready(stream, feed, opts)
    })
  }

  return stream
}

function Peer (feed) {
  events.EventEmitter.call(this)

  this._tick = 0
  this._index = -1
  this._blocks = 0

  this.uploading = true
  this.downloading = true
  this.maxRequests = maxRequests(feed)
  this.feed = feed
  this.channel = null
  this.stream = null

  this.remotePausing = false
  this.remoteBitfield = bitfield(16)
  this.remoteTree = tree(16)
  this.remoteRequests = []
  this.reading = false

  this.pausing = false
  this.responses = []
  this.requests = []
}

inherits(Peer, events.EventEmitter)

// called by the feed when it has a new block

Peer.prototype.have = function (block) {
  if (this.channel.opened || !this.uploading) this.channel.have({start: block})
}

// expose the update methods for easier debugging

Peer.prototype.update = function () {
  if (!this.downloading || this.remotePausing) return
  this.maxRequests = maxRequests(this.feed)

  while (true) {
    if (this.requests.length >= this.maxRequests) return

    var message = this.feed.selection.next(this.remoteBitfield, this._blocks)
    if (!message) return

    this.requests.push(message.block)
    this.channel.request(message)
  }
}

Peer.prototype.updateAll = function () {
  for (var i = 0; i < this.feed.peers.length; i++) {
    this.feed.peers[i].update()
  }
}

function ready (stream, feed, opts) {
  if (stream.destroyed) return

  var peer = new Peer(feed)
  var channel = peer.channel = stream.open(feed.key, {state: peer})

  peer.uploading = opts.upload !== false
  peer.downloading = opts.download !== false
  peer.stream = stream
  unordered.add(feed.peers, peer)
  feed.emit('peer-add', peer)

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

  if (!peer.uploading) {
    peer.channel.pause()
  } else {
    peer.channel.have({
      start: 0,
      bitfield: rle.encode(peer.feed.bitfield.toBuffer())
    })
  }
}

function onwant (message) {
  var peer = this.state
  var offset = Math.floor(message.start / 8) * 8

  peer.channel.have({
    start: offset,
    bitfield: rle.encode(this.feed.bitfield.toBuffer(offset))
  })
}

function onhave (message) {
  var peer = this.state

  // TODO: we should protect better / make configurable how big the BF grows

  var start = message.start
  var end = message.end || (start + 1)
  var blocks = 0

  if (message.bitfield) {
    var bits = bitfield(rle.decode(message.bitfield))

    for (var i = 0; i < bits.length; i++) {
      var set = bits.get(i)
      peer.remoteBitfield.set(start + i, set)
      if (set) blocks = start + i + 1
    }
  } else {
    while (start < end) {
      peer.remoteBitfield.set(start++, true)
    }
    blocks = end
  }

  if (blocks > peer._blocks) peer._blocks = blocks
  peer.update() // TODO: if previous update failed we should only look in the update state
}

function onrequest (message) {
  var peer = this.state
  if (peer.remoteRequests.push(message) === 1) read(peer)
}

function ondata (message) {
  var peer = this.state

  if (remove(peer.requests, message.block) === 0) peer._tick = 0
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

function maxRequests (feed) { // 512kb buffer
  if (!feed.blocks) return 16
  var avg = Math.ceil(feed.bytes / feed.blocks) // maybe use a better avg?
  var buffer = 256 * 1024 // arbitrary ... make customizable
  return Math.max(16, Math.min(Math.ceil(buffer / avg), 1024))
}

function onpause (message) {
  var peer = this.state

  peer.remotePausing = true
  while (peer.requests.length) {
    peer.feed.selection.cancel(peer.requests.shift())
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

  peer.feed.selection.cancel(peer.requests.shift())
  peer.updateAll()
}

function onend () {
  var peer = this.state

  while (peer.requests.length) {
    peer.feed.selection.cancel(peer.requests.shift())
  }

  unordered.remove(peer.feed.peers, peer)
  peer.feed.emit('peer-remove', peer)
  peer.updateAll()
  peer.emit('close')
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

    self.feed.emit('download', next.block, next.value, self)
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
      next.bytes = 0
      read(self)
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

  // TODO: support hash responses

  var opts = {digest: next.nodes, tree: self.remoteTree}
  if (next.bytes) return readBytes(self)

  if (self.reading) return
  self.reading = true

  self.feed.proof(next.block, opts, function (err, proof) {
    if (err) return self.channel.destroy(err)

    self.feed.get(next.block, function (err, data) {
      if (err) return self.channel.destroy(err)

      self.remoteBitfield.set(next.block, true)
      for (var i = 0; i < proof.nodes.length; i++) self.remoteTree.set(proof.nodes[i].index)
      self.remoteTree.set(2 * next.block)
      self.feed.emit('upload', next.block, data, self)

      self.channel.data({
        block: next.block,
        value: data,
        nodes: proof.nodes,
        signature: proof.signature
      })

      self.remoteRequests.shift()
      self.reading = false
      if (self.remoteRequests.length) read(self)
    })
  })
}
