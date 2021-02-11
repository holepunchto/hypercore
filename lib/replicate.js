var Protocol = require('hypercore-protocol')
var timeout = require('timeout-refresh')
var bitfield = require('fast-bitfield')
var set = require('unordered-set')
var rle = require('bitfield-rle').align(4)
var treeIndex = require('./tree-index')

var EMPTY = new Uint8Array(1024)

module.exports = replicate

function replicate (feed, initiator, opts) {
  feed.ifAvailable.wait()
  var stream = Protocol.isProtocolStream(initiator) ? initiator : opts.stream

  if (!stream) {
    if (!opts.keyPair) opts.keyPair = feed.noiseKeyPair
    stream = new Protocol(initiator, opts)
  }

  if (feed.opened) onready(null)
  else feed.ready(onready)

  return stream

  function onready (err) {
    feed.ifAvailable.continue()

    if (err) return stream.destroy(err)
    if (stream.destroyed) return
    if (stream.opened(feed.key)) return

    if (opts.noise !== false) {
      if (stream.remoteOpened(feed.key) && !stream.remoteVerified(feed.key)) {
        stream.close(feed.discoveryKey)
        return
      }
    }

    var peer = new Peer(feed, opts)

    peer.feed = feed
    peer.stream = stream.open(feed.key, peer)

    stream.setMaxListeners(0)
    peer.ready()
  }
}

function Peer (feed, opts) {
  if (opts.extensions) throw new Error('Per peer extensions is not supported. Use feed.registerExtension instead')

  this.feed = feed
  this.stream = null // set by replicate just after creation
  this.wants = bitfield()
  this.remoteBitfield = bitfield()
  this.remoteLength = 0
  this.remoteWant = false
  this.remoteTree = null
  this.remoteAck = false
  this.remoteOpened = false
  this.live = !!opts.live
  this.sparse = feed.sparse
  this.ack = !!opts.ack

  this.remoteDownloading = true
  this.remoteUploading = true
  this.remoteExtensions = feed.extensions.remote()
  this.downloading = typeof opts.download === 'boolean' ? opts.download : feed.downloading
  this.uploading = typeof opts.upload === 'boolean' ? opts.upload : feed.uploading

  this.updated = false

  this.maxRequests = opts.maxRequests || feed.maxRequests || 16
  this.urgentRequests = this.maxRequests + 16
  this.inflightRequests = []
  this.inflightWants = 0

  this._index = -1
  this._lastBytes = 0
  this._first = true
  this._closed = false
  this._destroyed = false
  this._defaultDownloading = this.downloading
  this._iterator = this.remoteBitfield.iterator()
  this._requestTimeout = null

  this.stats = !opts.stats ? null : {
    uploadedBytes: 0,
    uploadedBlocks: 0,
    downloadedBytes: 0,
    downloadedBlocks: 0
  }
}

Object.defineProperty(Peer.prototype, 'remoteAddress', {
  enumerable: true,
  get: function () {
    return this.stream.stream.remoteAddress
  }
})

Object.defineProperty(Peer.prototype, 'remoteType', {
  enumerable: true,
  get: function () {
    return this.stream.stream.remoteType
  }
})

Object.defineProperty(Peer.prototype, 'remotePublicKey', {
  enumerable: true,
  get: function () {
    return this.stream.state.remotePublicKey
  }
})

Peer.prototype.onwant = function (want) {
  if (!this.uploading) return
  // We only reploy to multipla of 8192 in terms of offsets and lengths for want messages
  // since this is much easier for the bitfield, in terms of paging.
  if ((want.start & 8191) || (want.length & 8191)) return
  if (!this.remoteWant && this.feed.length && this.feed.bitfield.get(this.feed.length - 1)) {
    // Eagerly send the length of the feed to the otherside
    // TODO: only send this if the remote is not wanting a region
    // where this is contained in
    this.stream.have({ start: this.feed.length - 1 })
  }
  this.remoteWant = true
  var rle = this.feed.bitfield.compress(want.start, want.length)
  this.stream.have({ start: want.start, length: want.length, bitfield: rle })
}

Peer.prototype.ondata = function (data) {
  var self = this

  // Ignore unrequested messages unless we allow push
  // TODO: would be better to check if the byte range was requested instead, but this works fine
  var allowPush = this.feed.allowPush || !data.value
  if (!allowPush && !this.feed._reserved.get(data.index)) {
    // If we do not have this block, send back unhave message for this index,
    // to let the remote know we rejected it.
    // TODO: we might want to have some "unwanted push" threshold to punish spammers
    if (!self.feed.bitfield.get(data.index)) self.unhave({ start: data.index })
    self._clear(data.index, !data.value)
    return
  }

  this.feed._putBuffer(data.index, data.value, data, this, function (err) {
    if (err) return self.destroy(err)
    if (data.value) self.remoteBitfield.set(data.index, false)
    if (self.remoteAck) {
      // Send acknowledgement.
      // In the future this could batch several ACKs at once
      self.stream.have({ start: data.index, length: 1, ack: true })
    }
    if (self.stats && data.value) {
      self.stats.downloadedBlocks += 1
      self.stats.downloadedBytes += data.value.length
    }
    self._clear(data.index, !data.value)
  })
}

Peer.prototype._clear = function (index, hash) {
  // TODO: optimize me (no splice and do not run through all ...)
  for (var i = 0; i < this.inflightRequests.length; i++) {
    if (this.inflightRequests[i].index === index) {
      if (this._requestTimeout !== null) this._requestTimeout.refresh()
      this.inflightRequests.splice(i, 1)
      i--
    }
  }

  this.feed._reserved.set(index, false)
  // TODO: only update all if we have overlapping selections
  this.feed._updatePeers()

  if (this.inflightRequests.length === 0 && this._requestTimeout !== null) {
    this._requestTimeout.destroy()
    this._requestTimeout = null
  }
}

Peer.prototype.onrequest = function (request) {
  if (!this.uploading) return
  if (request.bytes) return this._onbytes(request)

  // lazily instantiate the remote tree
  if (!this.remoteTree) this.remoteTree = treeIndex()

  var self = this
  var opts = { digest: request.nodes, hash: request.hash, tree: this.remoteTree }

  this.feed.proof(request.index, opts, onproof)

  function onproof (err, proof) {
    if (err) return self.destroy(err)
    if (request.hash) onvalue(null, null)
    else if (self.feed.bitfield.get(request.index)) self.feed._getBuffer(request.index, onvalue)

    function onvalue (err, value) {
      if (!self.uploading) return
      if (err) return self.destroy(err)

      if (value) {
        if (self.stats) {
          self.stats.uploadedBlocks += 1
          self.stats.uploadedBytes += value.length
          self.feed._stats.uploadedBlocks += 1
          self.feed._stats.uploadedBytes += value.length
        }
        self.feed.emit('upload', request.index, value, self)
      }

      // TODO: prob not needed with new bitfield
      if (request.index + 1 > self.remoteLength) {
        self.remoteLength = request.index + 1
        self._updateEnd()
      }

      self.stream.data({
        index: request.index,
        value: value,
        nodes: proof.nodes,
        signature: proof.signature
      })
    }
  }
}

Peer.prototype._updateOptions = function () {
  if (this.ack || this.feed.extensions.length) {
    this.stream.options({
      ack: this.ack,
      extensions: this.feed.extensions.names()
    })
  }
}

Peer.prototype.setDownloading = function (downloading) {
  if (downloading === this.downloading) return
  this.downloading = downloading
  this.stream.status({
    downloading,
    uploading: this.uploading
  })
  this.update()
}

Peer.prototype.setUploading = function (uploading) {
  if (uploading === this.uploading) return
  this.uploading = uploading
  this.stream.status({
    downloading: this.downloading,
    uploading
  })
  this.update()
}

Peer.prototype._onbytes = function (request) {
  var self = this

  this.feed.seek(request.bytes, { wait: false }, function (err, index) {
    if (err) {
      request.bytes = 0
      self.onrequest(request)
      return
    }

    // quick'n'dirty filter for parallel bytes requests
    // it does not matter that this doesn't catch ALL parallel requests - just a bandwidth optimization
    if (self._lastBytes === request.bytes) return
    self._lastBytes = request.bytes

    request.bytes = 0
    request.index = index
    request.nodes = 0

    self.onrequest(request)
  })
}

Peer.prototype._onrequesttimeout = function () {
  this._requestTimeout = null
  if (!this.inflightRequests.length) return

  var first = this.inflightRequests[0]

  if (first.hash ? this.feed.tree.get(2 * first.index) : this.feed.bitfield.get(first.index)) {
    // prob a bytes response
    this.inflightRequests.shift()
    this.feed._reserved.set(first.index, false)

    if (this.stream.stream.timeout) {
      this._requestTimeout = timeout(this.stream.stream.timeout.ms, this._onrequesttimeout, this)
    }
    return
  }

  this.destroy(new Error('Request timeout'))
}

Peer.prototype.onhave = function (have) {
  this.feed.emit('peer-ack', this, have)

  if (this.ack && have.ack && !have.bitfield && this.feed.bitfield.get(have.start)) {
    this.stream.stream.emit('ack', have)
    return
  }

  var updated = this._first
  if (this._first) this._first = false

  // In this impl, we only sent WANTs for 1024 * 1024 length ranges
  // so if we get a HAVE for that it is a reply to a WANT.
  if (have.length === 1024 * 1024 && this.inflightWants > 0) {
    this.feed.ifAvailable.continue()
    this.inflightWants--
  }

  if (have.bitfield) { // TODO: handle start !== 0
    if (have.length === 0 || have.length === 1) { // length === 1 is for backwards compat
      this.wants = null // we are in backwards compat mode where we subscribe everything
    }
    var buf = rle.decode(have.bitfield)
    var bits = buf.length * 8
    remoteAndNotLocal(this.feed.bitfield, buf, this.remoteBitfield.littleEndian, have.start)
    this.remoteBitfield.fill(buf, have.start)
    if (bits > this.remoteLength) {
      this.remoteLength = this.remoteBitfield.last() + 1
      updated = true
    }
  } else {
    // TODO: if len > something simply copy a 0b1111... buffer to the bitfield

    var start = have.start
    var len = have.length || 1

    while (len--) this.remoteBitfield.set(start, !this.feed.bitfield.get(start++))
    if (start > this.remoteLength) {
      this.remoteLength = start
      updated = true
    }
  }

  if (updated) {
    this.updated = true
    this.feed.emit('remote-update', this)
  }

  this._updateEnd()
  this.update()
}

Peer.prototype._updateEnd = function () {
  if (this.live || this.feed.sparse || !this.feed._selections.length) return

  var sel = this.feed._selections[0]
  var remoteLength = this.feed.length || -1

  for (var i = 0; i < this.feed.peers.length; i++) {
    if (this.feed.peers[i].remoteLength > remoteLength) {
      remoteLength = this.feed.peers[i].remoteLength
    }
  }

  sel.end = remoteLength
}

Peer.prototype.onextension = function (id, message) {
  this.remoteExtensions.onmessage(id, message, this)
}

Peer.prototype.onstatus = function (info) {
  this.remoteUploading = info.uploading
  this.remoteDownloading = info.downloading

  if (!info.uploading) {
    while (this.inflightRequests.length) {
      const data = this.inflightRequests[0]
      this._clear(data.index, !data.value)
    }
    for (var i = 0; i < this.inflightWants; i++) {
      this.feed.ifAvailable.continue()
    }
    this.inflightWants = 0
    this.wants = bitfield()
  }
  this.update()
  if (info.downloading || this.live) return
  if (this.feed._selections.length && this.downloading) return
  this._autoEnd()
}

Peer.prototype._autoEnd = function () {
  if (this.uploading && this.remoteDownloading) return
  if ((this.sparse || this.live) && (this.remoteUploading || this.downloading)) return
  this.end()
}

Peer.prototype.onunhave = function (unhave) {
  var start = unhave.start
  var len = unhave.length || 1

  if (start === 0 && len >= this.remoteLength) {
    this.remoteLength = 0
    this.remoteBitfield = bitfield()
    return
  }

  while (len--) this.remoteBitfield.set(start++, false)
}

Peer.prototype.onunwant =
Peer.prototype.oncancel = function () {
  // TODO: impl all of me
}

Peer.prototype.onclose = function () {
  this._close()
}

Peer.prototype.have = function (have) { // called by feed
  if (this.stream && this.remoteWant) this.stream.have(have)
  var start = have.start
  var len = have.length
  while (len--) this.remoteBitfield.set(start++, false)
}

Peer.prototype.unhave = function (unhave) { // called by feed
  if (this.stream && this.remoteWant) this.stream.unhave(unhave)
}

Peer.prototype.haveBytes = function (bytes) { // called by feed
  for (var i = 0; i < this.inflightRequests.length; i++) {
    if (this.inflightRequests[i].bytes === bytes) {
      this.feed._reserved.set(this.inflightRequests[i].index, false)
      this.inflightRequests.splice(i, 1)
      i--
    }
  }

  this.update()

  if (this.inflightRequests.length === 0 && this._requestTimeout !== null) {
    this._requestTimeout.destroy()
    this._requestTimeout = null
  }
}

Peer.prototype.update = function () {
  // do nothing
  while (this._update()) {}
  this._sendWantsMaybe()
}

Peer.prototype._update = function () {
  // should return true if mutated false if not
  if (!this.downloading || !this.remoteUploading) return false
  var selections = this.feed._selections
  var waiting = this.feed._waiting
  var wlen = waiting.length
  var slen = selections.length
  var inflight = this.inflightRequests.length
  var offset = 0
  var i = 0

  // TODO: less duplicate code here
  // TODO: re-add priority levels

  while (inflight < this.urgentRequests) {
    offset = Math.floor(Math.random() * waiting.length)

    for (i = 0; i < waiting.length; i++) {
      var w = waiting[offset++]
      if (offset === waiting.length) offset = 0

      this._downloadWaiting(w)
      if (waiting.length !== wlen) return true // mutated
      if (this.inflightRequests.length >= this.urgentRequests) return false
    }
    if (inflight === this.inflightRequests.length) break
    inflight = this.inflightRequests.length
  }

  while (inflight < this.maxRequests) {
    offset = Math.floor(Math.random() * selections.length)

    for (i = 0; i < selections.length; i++) {
      var s = selections[offset++]
      if (offset === selections.length) offset = 0

      if (!s.iterator) s.iterator = this.feed.bitfield.iterator(s.start, s.end)
      if (s.blocks) this._downloadBlocks(s)
      else this._downloadRange(s)
      if (selections.length !== slen) return true // mutated
      if (this.inflightRequests.length >= this.maxRequests) return false
    }

    if (inflight === this.inflightRequests.length) return false
    inflight = this.inflightRequests.length
  }

  return false
}

Peer.prototype.onopen = function () {
  this.feed.ifAvailable.continue()
  this.remoteOpened = true

  this._updateOptions()

  if (!this.uploading || !this.downloading) {
    this.stream.status({
      uploading: this.uploading,
      downloading: this.downloading
    })
  }

  this._sendWants()
  this.feed.emit('peer-open', this)
}

Peer.prototype.onoptions = function (options) {
  this.remoteAck = options.ack
  this.remoteExtensions.update(options.extensions)
}

Peer.prototype.ready = function () {
  this.feed.ifAvailable.wait() // continued by onopen or close
  set.add(this.feed.peers, this)
  this.feed.emit('peer-add', this)
  if (this.stream.remoteOpened) this.onopen()
}

Peer.prototype.end = function () {
  if (!this.downloading && !this.remoteDownloading && !this.live) {
    if (!this._defaultDownloading) {
      this.stream.status({ downloading: false, uploading: false })
    }
    this._close()
    return
  }
  if (!this._closed) {
    this._closed = true
    this.downloading = false
    this.stream.status({ downloading: false, uploading: true })
  } else {
    if (!this.live) this._close()
  }
}

Peer.prototype._close = function () {
  if (!this._destroyed) {
    this._destroyed = true
    this.stream.close()
  }
  if (this._index === -1) return
  set.remove(this.feed.peers, this)
  this._index = -1
  for (var i = 0; i < this.inflightRequests.length; i++) {
    this.feed._reserved.set(this.inflightRequests[i].index, false)
  }
  if (this._requestTimeout !== null) {
    this._requestTimeout.destroy()
    this._requestTimeout = null
  }
  this._updateEnd()
  this.remoteWant = false
  this.feed._updatePeers()
  this.feed.emit('peer-remove', this)
  for (i = 0; i < this.inflightWants; i++) {
    this.feed.ifAvailable.continue()
  }
  if (!this.remoteOpened) {
    this.feed.ifAvailable.continue()
  }
}

Peer.prototype.destroy = function (err) {
  if (this._index === -1 || this._destroyed) return
  this.stream.destroy(err)
  this._destroyed = true
  this._close()
}

Peer.prototype._sendWantsMaybe = function () {
  if (this.inflightRequests.length < this.urgentRequests) this._sendWants()
}

Peer.prototype._sendWants = function () {
  if (!this.wants || !this.downloading || !this.remoteOpened || !this.remoteUploading) return
  if (this.inflightWants >= 16) return

  var i

  for (i = 0; i < this.feed._waiting.length; i++) {
    var w = this.feed._waiting[i]
    if (w.index === -1) this._sendWantRange(w)
    else this._sendWant(w.index)
    if (this.inflightWants >= 16) return
  }

  for (i = 0; i < this.feed._selections.length; i++) {
    var s = this.feed._selections[i]
    this._sendWantRange(s)
    if (this.inflightWants >= 16) return
  }

  // always sub to the first range for now, usually what you want
  this._sendWant(0)
}

Peer.prototype._sendWantRange = function (s) {
  if (s.blocks) {
    if (!s.selected) s.selected = new WeakSet()
    if (s.selected.has(this)) return
    s.selected.add(this)
    for (const block of s.blocks) {
      this._sendWant(block)
    }
    return
  }

  var want = s.start ? 1024 * 1024 * Math.floor(s.start / 1024 / 1024) : 0

  while (true) {
    if (want >= this.remoteLength) return
    if (s.end !== -1 && want >= s.end) return

    if (this._sendWant(want)) return

    // check if region is already selected - if so try next one
    if (!this.wants.get(Math.floor(want / 1024 / 1024))) return
    want += 1024 * 1024
  }
}

Peer.prototype._sendWant = function (index) {
  var len = 1024 * 1024
  var j = Math.floor(index / len)
  if (this.wants.get(j)) return false
  this.wants.set(j, true)
  this.inflightWants++
  this.feed.ifAvailable.wait()
  this.stream.want({ start: j * len, length: len })
  return true
}

Peer.prototype._downloadWaiting = function (wait) {
  if (!wait.bytes) {
    if (!this.remoteBitfield.get(wait.index) || !this.feed._reserved.set(wait.index, true)) {
      if (!wait.update || this.feed._reserved.get(wait.index)) return
      const i = this._iterator.seek(wait.index).next(true)
      if (i === -1 || !this.feed._reserved.set(i, true)) return
      wait.index = i
    }
    this._request(wait.index, 0, wait.hash === true)
    return
  }

  this._downloadRange(wait)
}

Peer.prototype._downloadBlocks = function (range) {
  while (range.blocksDownloaded < range.blocks.length) {
    const blk = range.blocks[range.blocksDownloaded]
    if (!this.feed.bitfield.get(blk)) break
    range.blocksDownloaded++
  }

  if (range.blocksDownloaded >= range.blocks.length) {
    set.remove(this.feed._selections, range)
    range.callback(null)
    return
  }

  for (var i = range.blocksDownloaded; i < range.blocks.length; i++) {
    const blk = range.blocks[i]
    if (this.remoteBitfield.get(blk) && this.feed._reserved.set(blk, true)) {
      range.requested++
      this._request(blk, 0, false)
      return
    }
  }
}

Peer.prototype._downloadRange = function (range) {
  if (!range.iterator) range.iterator = this.feed.bitfield.iterator(range.start, range.end)

  var reserved = this.feed._reserved
  var ite = this._iterator
  var wantedEnd = Math.min(range.end === -1 ? this.remoteLength : range.end, this.remoteLength)

  var i = range.linear ? ite.seek(range.start).next(true) : nextRandom(ite, range.start, wantedEnd)
  var start = i

  if (i === -1 || i >= wantedEnd) {
    if (!range.bytes && range.end > -1 && this.feed.length >= range.end && range.iterator.seek(0).next() === -1) {
      set.remove(this.feed._selections, range)
      range.callback(null)
      if (!this.live && !this.sparse && !this.feed._selections.length) this.end()
    }
    return
  }

  while ((range.hash && this.feed.tree.get(2 * i)) || !reserved.set(i, true)) {
    i = ite.next(true)

    if (i > -1 && i < wantedEnd) {
      // check this index
      continue
    }

    if (!range.linear && start !== 0) {
      // retry from the beginning since we are iterating randomly and started !== 0
      i = ite.seek(range.start).next(true)
      start = 0
      if (i > -1 && i < wantedEnd) continue
    }

    // we have checked all indexes.
    // if we are looking for hashes we should check if we have all now (first check only checks blocks)
    if (range.hash) {
      // quick'n'dirty check if have all hashes - can be optimized be checking only tree roots
      // but we don't really request long ranges of hashes so yolo
      for (var j = range.start; j < wantedEnd; j++) {
        if (!this.feed.tree.get(2 * j)) return
      }
      if (!range.bytes) {
        set.remove(this.feed._selections, range)
        range.callback(null)
      }
    }

    // exit the update loop - nothing to do
    return
  }

  range.requested++
  this._request(i, range.bytes || 0, range.hash)
}

Peer.prototype._request = function (index, bytes, hash) {
  var request = {
    bytes: bytes,
    index: index,
    hash: hash,
    nodes: this.feed.digest(index)
  }

  if (this._requestTimeout === null && this.stream.stream.timeout) {
    this._requestTimeout = timeout(this.stream.stream.timeout.ms, this._onrequesttimeout, this)
  }
  this.inflightRequests.push(request)
  this.stream.request(request)
}

Peer.prototype.extension = function (id, message) {
  this.stream.extension(id, message)
}

function createView (page) {
  var buf = page ? page.buffer : EMPTY
  return new DataView(buf.buffer, buf.byteOffset, 1024)
}

function remoteAndNotLocal (local, buf, le, start) {
  var remote = new DataView(buf.buffer, buf.byteOffset)
  var len = Math.floor(buf.length / 4)
  var arr = new Uint32Array(buf.buffer, buf.byteOffset, len)
  var p = start / 8192 // 8192 is bits per bitfield page
  var l = 0
  var page = createView(local.pages.get(p++, true))

  for (var i = 0; i < len; i++) {
    arr[i] = remote.getUint32(4 * i, !le) & ~page.getUint32(4 * (l++), !le)

    if (l === 256) {
      page = createView(local.pages.get(p++, true))
      l = 0
    }
  }
}

function nextRandom (ite, start, end) {
  var len = end - start
  var i = ite.seek(Math.floor(Math.random() * len) + start).next(true)
  return i === -1 || i >= end ? ite.seek(start).next(true) : i
}
