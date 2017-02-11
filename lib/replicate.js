var bitfield = require('sparse-bitfield')
var protocol = require('hypercore-protocol')
var set = require('unordered-set')
var remove = require('unordered-array-remove')
var rle = require('bitfield-rle')

module.exports = replicate

function replicate (feed, stream) {
  if (!stream) stream = protocol({id: feed.id})

  var peer = new Peer()

  peer.stream = stream
  peer.feed = feed

  stream.on('close', function () {
    if (peer.channel) peer.destroy()
  })

  feed.ready(function (err) {
    if (stream.destroyed) return
    if (err) return stream.destroy(err)
    if (!feed.key) return stream.destroy(new Error('Finalize static feed before replicating'))

    set.add(feed._peers, peer)
    peer.channel = stream.open(feed.key)
    peer.channel.state = peer

    peer.channel.on('have', onhave)
    peer.channel.on('want', onwant)
    peer.channel.on('request', onrequest)
    peer.channel.on('data', ondata)

    peer.channel.have({
      start: 0,
      bitfield: rle.encode(feed.bitfield.toBuffer())
    })

    peer.update()
  })

  return stream
}

function Peer () {
  this.stream = null
  this.feed = null
  this.channel = null
  this.remoteBitfield = bitfield()
  this.destroyed = false

  this._downloading = 0
  this._index = 0 // for the set
  this._reserved = bitfield()
}

Peer.prototype.have = function (message) {
  this.channel.have(message)
}

Peer.prototype.update = function () {
  var selection = this.feed._selection
  var hwm = 128

  for (var i = 0; i < selection.length; i++) {
    if (this._downloading > hwm) return

    var s = selection[i]

    if (s.blocks) {
      for (var j = s.downloaded; j < s.blocks.length; j++) {
        if (s.ptr === s.blocks.length) s.ptr = s.downloaded

        var blk = s.blocks[s.ptr++]
        if (!this.remoteBitfield.get(blk) || this.feed.has(blk)) continue
        if (!this._reserved.set(blk, true)) continue

        this.channel.request({
          block: blk,
          nodes: this.feed.tree.digest(2 * blk)
        })

        if (++this._downloading > hwm) return
      }
    } else {
      if (!this.remoteBitfield.get(s.index)) continue

      if (this._reserved.get(s.index)) continue
      this._reserved.set(s.index, true)

      this.channel.request({
        block: s.index,
        nodes: this.feed.tree.digest(2 * s.index)
      })

      if (++this._downloading > hwm) return
    }
  }
}

Peer.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  set.remove(this.feed._peers, this)
  this.stream.destroy(err)
}

function onhave (message) {
  var peer = this.state

  if (message.bitfield) { // TODO: support .start
    var bitfield = rle.decode(message.bitfield)
    peer.remoteBitfield.setBuffer(0, bitfield)
  } else {
    var start = message.start
    var end = message.end || start + 1
    while (start < end) {
      peer.remoteBitfield.set(start++, true)
    }
  }

  peer.update()
}

function onwant () {

}

function onrequest (data) {
  var peer = this.state

  peer.feed.proof(data.block, {digest: data.nodes}, function (err, proof) {
    if (err) return peer.destroy(err)
    peer.feed._storage.getData(data.block, function (err, buffer) {
      if (err) return peer.destroy(err)

      peer.remoteBitfield.set(data.block, true)
      peer.channel.data({
        block: data.block,
        value: buffer,
        nodes: proof.nodes,
        signature: proof.signature
      })

      peer.feed.emit('upload', data.block, buffer, proof.nodes)
    })
  })
}

function ondata (data) {
  var peer = this.state

  peer.feed._putBuffer(data.block, data.value, data, peer, function (err) {
    if (err) return peer.destroy(err)
    drain(peer, data.block)
  })
}

function drain (peer, block) {
  peer._downloading-- // TODO: not completely correct

  for (var i = 0; i < peer.feed._selection.length; i++) {
    var s = peer.feed._selection[i]

    if (s.blocks) {
      while (s.downloaded < s.blocks.length && peer.feed.has(s.blocks[s.downloaded])) {
        s.downloaded++
      }
      if (s.downloaded === s.blocks.length) {
        remove(peer.feed._selection, i--)
        if (s.callback) s.callback(null)
      }
      continue
    }

    if (s.index !== block) continue

    remove(peer.feed._selection, i--)

    if (s.get) {
      peer.feed.get(s.index, s.callback)
    } else if (s.callback) {
      s.callback(null)
    }
  }

  peer.update()
}
