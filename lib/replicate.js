var protocol = require('hypercore-protocol')
var bitfield = require('./bitfield')
var rle = require('bitfield-rle')
var unordered = require('unordered-set')
var inherits = require('inherits')
var events = require('events')
var prettyHash = require('pretty-hash')
var tree = require('./tree-index')
var debug = require('debug')('hypercore-replicate')

module.exports = replicate
module.exports.unreplicate = unreplicate

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

function unreplicate (core, feed, stream) {
  if (!feed) throw new Error('Feed must be provided to unreplicate()')

  for (var i = 0; i < feed.peers.length; i++) {
    // if a specific stream was provided, only disconnect that stream
    if (stream && feed.peers[i].stream !== stream) continue

    feed.peers[i].disconnect()
  }
}

function Peer (feed) {
  events.EventEmitter.call(this)

  this._tick = 0
  this._index = -1
  this._blocks = 0

  this.remoteLength = 0
  this.bytesDownloaded = 0
  this.downloaded = 0

  this.uploading = true
  this.downloading = !feed.secretKey
  this.maxRequests = maxRequests(feed)
  this.maxResponses = this.maxRequests * 2
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
  if (this.channel.opened || !this.uploading) {
    this.debug('have() sending to peer')
    this.channel.have({start: block})
  }
}

// expose the update methods for easier debugging

Peer.prototype.update = function () {
  if (!this.downloading || this.remotePausing || this.channel.closed) return
  this.maxRequests = maxRequests(this.feed)
  this.maxResponses = this.maxRequests * 2

  while (true) {
    if (this.requests.length >= this.maxRequests) {
      this.debug('update() stopping, too many requests queued (%d)', this.maxRequests)
      return
    }
    if (this.responses.length >= this.maxResponses) {
      this.debug('update() stopping, too many responses queued (%d)', this.maxResponses)
      return
    }

    var message = this.feed.selection.next(this.remoteBitfield, this._blocks)
    if (!message) {
      this.debug('update() stopping, no more messages wanted')
      return
    }

    this.debug('update() requesting block=%d', message.block)
    this.requests.push(message.block)
    this.channel.request(message)
  }
}

Peer.prototype.updateAll = function () {
  for (var i = 0; i < this.feed.peers.length; i++) {
    this.feed.peers[i].update()
  }
}

Peer.prototype.sendUnhaveToAll = function (msg) {
  for (var i = 0; i < this.feed.peers.length; i++) {
    this.feed.peers[i].channel.unhave(msg)
  }
}

Peer.prototype.disconnect = function () {
  this.debug('disconnect()')
  this.channel.close()
}

Peer.prototype.debug = function () {
  if (!debug.enabled) return
  var args = [].slice.call(arguments)
  if (this.channel) args[0] = 'chan=' + prettyHash(this.channel.discoveryKey) + ' ' + args[0]
  debug.apply(debug, args)
}

function ready (stream, feed, opts) {
  if (stream.destroyed) {
    debug('ready() called after stream was destroyed, aborting')
    return
  }

  var peer = new Peer(feed)
  var channel = peer.channel = stream.open(feed.key, {state: peer})
  peer.debug('ready()')

  peer.uploading = opts.upload !== false
  peer.downloading = opts.download !== false
  peer.stream = stream
  unordered.add(feed.peers, peer)
  feed.emit('peer-add', peer)

  channel.on('have', onhave)
  channel.on('unhave', onunhave)
  channel.on('want', onwant)
  channel.on('request', onrequest)
  channel.on('data', ondata)
  channel.on('cancel', oncancel)
  channel.on('pause', onpause)
  channel.on('resume', onresume)

  // meta events
  channel.on('tick', ontick)
  channel.on('close', onclose)

  if (channel.opened) {
    peer.debug('channel already open')
    onopen.call(channel)
  } else {
    peer.debug('waiting for channel to open')
    channel.on('open', onopen)
  }
}

function onopen () {
  var peer = this.state

  if (!peer.uploading) {
    peer.debug('onopen() peer is not uploading, pausing channel')
    peer.channel.pause()
  } else {
    peer.debug('onopen() peer is uploading, sending have msg')
    peer.channel.have({
      start: 0,
      bitfield: rle.encode(peer.feed.bitfield.toBuffer())
    })
  }
}

function onwant (message) {
  var peer = this.state
  var offset = Math.floor(message.start / 8) * 8
  peer.debug('onwant() sending have for msg %d', offset)

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
  peer.debug('onhave() start=%d end=%d blocks=%d', start, end, blocks)

  if (blocks > peer._blocks) {
    peer._blocks = blocks
    peer.remoteLength = blocks
  }

  peer.update() // TODO: if previous update failed we should only look in the update state
}

function onunhave (message) {
  var peer = this.state

  var start = message.start
  var end = message.end || (start + 1)
  peer.debug('onunhave() start=%d end=%d', start, end)

  while (start < end) {
    peer.remoteBitfield.set(start, false)
    if (remove(peer.requests, message.block) === 0) {
      peer.debug('tick reset (received top-of-queue request)')
      peer._tick = 0
    }
    start++
  }
  peer.update()
}

function onrequest (message) {
  var peer = this.state
  if (peer.remoteRequests.push(message) === 1) {
    peer.debug('onrequest() ')
    read(peer)
  }
}

function ondata (message) {
  var peer = this.state
  peer.debug('ondata()')

  if (remove(peer.requests, message.block) === 0) {
    peer.debug('tick reset (received top-of-queue request)')
    peer._tick = 0
  }
  if (peer.responses.push(message) === 1) write(peer)

  peer.update()
}

function oncancel (message) {
  var peer = this.state
  peer.debug('oncancel() block=%d bytes=%d', message.block, message.bytes)

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
  var buffer = 1024 * 1024 // arbitrary ... make customizable
  return Math.max(16, Math.min(Math.ceil(buffer / avg), 1024))
}

function onpause (message) {
  var peer = this.state
  peer.debug('onpause() cancelling %d requests and resetting tick', peer.requests.length)

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
  peer.debug('onresume()')

  peer.remotePausing = false
  peer.update()
}

function ontick () {
  var peer = this.state

  if (!peer.requests.length) return
  if (++peer._tick < 4) {
    peer.debug('ontick() now at %d', peer._tick)
    return
  }

  // timeout!

  peer.debug('ontick() timeout at %d, canceling a request', peer._tick)
  peer.feed.selection.cancel(peer.requests.shift())
  peer.updateAll()
}

function onclose () {
  var peer = this.state
  peer.debug('onclose() aborting %d requests', peer.requests.length)

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

  self.debug('write() block=%d', next.block)
  self.feed.put(next.block, next.value, next, function (err) {
    if (err) {
      self.debug('write() error', err)
      return destroy(self.channel, err)
    }

    self.downloaded++
    self.bytesDownloaded += next.value.length

    self.feed.emit('download', next.block, next.value, self)
    self.responses.shift()
    self.update()
    if (self.responses.length) write(self)
  })
}

function destroy (channel, err) {
  channel.protocol.destroy(err)
}

function readBytes (self) {
  var next = self.remoteRequests[0]

  self.debug('readBytes() bytes=%d', next.bytes)
  self.feed._seek(next.bytes, function (err, index) {
    if (err || index & 1) { // Could not fulfil byte request
      next.bytes = 0
      self.debug('readBytes() failed, trying again as a block read')
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
    self.debug('read() dropping request for block=%d, remote bitfield already has it', next.block)
    if (!self.remoteRequests.length) return
    next = self.remoteRequests[0]
  }

  // TODO: support hash responses

  var opts = {digest: next.nodes, tree: self.remoteTree}
  if (next.bytes) return readBytes(self)

  if (self.reading) {
    self.debug('read() aborting: already reading')
    return
  }
  self.reading = true

  self.feed.proof(next.block, opts, function (err, proof) {
    if (err) {
      self.debug('read() proof failed, aborting', err)
      return destroy(self.channel, err)
    }

    self.feed.get(next.block, { verify: self.feed.verifyReplicationReads }, function (err, data) {
      if (err) {
        if (err.notFound) {
          self.debug('read() verification failed, sending unhave')
          self.sendUnhaveToAll({ start: next.block })
          return loop()
        }
        self.debug('read() get failed, aborting', err)
        return destroy(self.channel, err)
      }

      self.remoteBitfield.set(next.block, true)
      for (var i = 0; i < proof.nodes.length; i++) self.remoteTree.set(proof.nodes[i].index)
      self.remoteTree.set(2 * next.block)
      self.feed.emit('upload', next.block, data, self)

      self.debug('read() sending block=%d', next.block)
      self.channel.data({
        block: next.block,
        value: data,
        nodes: proof.nodes,
        signature: proof.signature
      })

      loop()
    })
  })

  function loop () {
    self.remoteRequests.shift()
    self.reading = false
    if (self.remoteRequests.length) read(self)
  }
}
