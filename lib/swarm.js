var protocol = require('./protocol')
var debug = require('debug')('hyperdrive-swarm')

var MAX_INFLIGHT_PER_PEER = 20

module.exports = Swarm

function Swarm (core, opts) {
  if (!(this instanceof Swarm)) return new Swarm(core, opts)
  if (!opts) opts = {}
  this.prioritized = 0
  this.core = core
  this.peers = []
  this.feeds = []
  this.joined = {}
  this.kicking = false
  this.extensions = []
  this._streaming = false
}

Swarm.prototype.use = function (name) {
  if (this._streaming) throw new Error('Add all extensions before creating peer streams')
  this.extensions.push(name)
}

Swarm.prototype._kick = function () {
  if (this.kicking) return
  this.kicking = true
  debug('polling all peers to see if they have something to do')
  // TODO: optimize this process - will iterate too much atm
  var ids = Object.keys(this.joined)
  for (var i = 0; i < ids.length; i++) {
    this.joined[ids[i]].fetch()
  }
  this.kicking = false
}

Swarm.prototype._get = function (link) {
  var id = link.toString('hex')
  var self = this

  if (this.joined[id]) return this.joined[id]

  var subswarm = {
    id: id,
    feed: null,
    link: link,
    peers: [],
    fetch: fetch,
    open: open
  }

  if (this.core._opened[id]) open(this.core.get(link))
  this.joined[id] = subswarm
  return subswarm

  function open (feed) {
    if (subswarm.feed) return
    subswarm.feed = feed
    self.prioritized += subswarm.feed.want.length

    subswarm.feed.on('want', function (block) {
      self.prioritized++
      debug('prioritizing block %d (%d)', block, self.prioritized)
      subswarm.fetch()
    })

    subswarm.feed.on('unwant', function (block) {
      self.prioritized--
      debug('deprioritizing block %d (%d)', block, self.prioritized)
      if (!self.prioritized) subswarm.fetch()
    })

    subswarm.feed.on('put', function (block) {
      for (var i = 0; i < subswarm.peers.length; i++) {
        subswarm.peers[i].have(block)
      }
    })

    subswarm.peers.forEach(function (ch) {
      subswarm.feed.open(function (err) { // TODO: hackish for now
        if (err) return ch.leave(err)
        ch.bitfield(subswarm.feed.bitfield)
        ch.unpause() // for now always unpause. #yolo
        process.nextTick(function () {
          subswarm.fetch(ch)
        })
      })
    })
  }

  function fetch (peer) {
    if (!subswarm.feed || !subswarm.feed.opened) return
    debug('should try to fetch')

    if (peer) fetchPeer(peer)
    else subswarm.peers.forEach(fetchPeer)
  }

  function fetchPeer (peer) {
    debug('analyzing peer (inflight: %d, paused: %s)', peer.stream.inflight, peer.remotePausing)
    if (peer.remotePausing) return
    while (true) {
      if (peer.stream.inflight >= MAX_INFLIGHT_PER_PEER) return
      var block = chooseBlock(peer)
      if (block < 0) return self._kick() // nothing to do here - restart logic. TODO: improve me
      peer.request(block)
      debug('peer is fetching block %d', block)
    }
  }

  function chooseBlock (peer) {
    // TODO: maintain a bitfield of perswarm blocks in progress
    // so we wont fetch the same data from multiple peers
    if (!subswarm.feed) return -3 // feed half open

    var len = peer.remoteBitfield.buffer.length * 8
    var block = -1
    var critical = false

    for (var j = 0; j < subswarm.feed.want.length; j++) {
      block = subswarm.feed.want[j].block
      if (subswarm.feed.want[j].critical) critical = true
      if (peer.amRequesting.get(block)) continue
      if (peer.remoteBitfield.get(block) && !subswarm.feed.bitfield.get(block)) {
        debug('choosing prioritized block #%d', block)
        return block
      }
    }

    if (critical) {
      debug('not downloading any non-critical blocks')
      return -4
    }

    // TODO: there might be a starvation convern here. should only return *if* there are peers that
    // that could satisfy the want list. this is just a quick "hack" for realtime prioritization
    // when dealing with multiple files
    if (self.prioritized > 0 && !subswarm.feed.want.length) { // self.prioritized < 0 sometimes. that's a bug though :/
      debug('not downloading to yield to prioritized downloading')
      return -1
    }

    var prioritizedish = !!subswarm.feed.want.length
    var offset = prioritizedish ? subswarm.feed.want[0].block : ((Math.random() * len) | 0)
    for (var i = 0; i < len; i++) {
      block = (offset + i) % len
      if (peer.amRequesting.get(block)) continue
      if (peer.remoteBitfield.get(block) && !subswarm.feed.bitfield.get(block)) {
        if (!prioritizedish) debug('choosing unprioritized block #%d', block)
        else debug('choosing semi prioritized block #%d', block)
        return block
      }
    }

    debug('could not find a block to download')
    return -2
  }
}

Swarm.prototype.join = function (link) {
  var id = link.toString('hex')
  if (this.feeds.indexOf(id) === -1) this.feeds.push(id)

  for (var i = 0; i < this.peers.length; i++) {
    this.peers[i].join(link)
  }

  return this._get(link)
}

Swarm.prototype.createStream = function () {
  if (!this._streaming) {
    this._streaming = true
    this.extensions.sort()
  }

  var self = this
  var peer = protocol({id: this.id, extensions: self.extensions})

  debug('new peer stream')

  peer.on('channel', onchannel)
  peer.on('end', remove)
  peer.on('finish', remove)
  peer.on('close', remove)

  this.peers.push(peer)
  for (var i = 0; i < this.feeds.length; i++) {
    peer.join(new Buffer(this.feeds[i], 'hex'))
  }

  return peer

  function add (ch) {
    var subswarm = self._get(ch.link)
    subswarm.peers.push(ch)
    ch.on('leave', function () {
      var i = subswarm.peers.indexOf(ch)
      if (i > -1) subswarm.peers.splice(ch, 1)
    })
    return subswarm
  }

  function onchannel (ch) {
    var name = ch.link.toString('hex').slice(0, 12)
    var subswarm = add(ch)

    debug('[channel %s] joined channel', name)

    ch.on('unpause', function () {
      subswarm.fetch(ch)
    })

    ch.on('response', function (block, data, proof) {
      debug('[channel %s] rcvd response #%d (%d bytes, proof contained %d hashes)', name, block, data.length, proof.length)
      if (!subswarm.feed) return
      subswarm.fetch(ch)
      subswarm.feed.put(block, data, proof, function (err) {
        if (err) ch.leave(err)
      })
    })

    ch.on('request', function (block) {
      if (ch.amPausing) return
      debug('[channel %s] rcvd request #%d', name, block)
      if (!subswarm.feed) return
      subswarm.feed.get(block, function (err, data) {
        if (err) return ch.leave(err)
        if (!data) return ch.leave(new Error('Remote peer wants a block that is out of bounds'))
        subswarm.feed.proof(block, function (err, proof) {
          if (err) return ch.leave(err)
          ch.response(block, data, proof)
        })
      })
    })

    ch.on('warn', function (err) {
      debug('[channel %s] warning "%s"', name, err.message)
    })

    ch.on('have', function () {
      subswarm.fetch(ch)
    })

    if (!subswarm.feed) return
    subswarm.feed.open(function (err) {
      if (err) return ch.leave(err)
      ch.bitfield(subswarm.feed.bitfield)
      ch.unpause() // for now always unpause. #yolo
      process.nextTick(function () {
        subswarm.fetch(ch)
      })
    })
  }

  function remove () {
    var i = self.peers.indexOf(peer)
    if (i > -1) self.peers.splice(i, 1)
  }
}
