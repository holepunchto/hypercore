const b4a = require('b4a')
const safetyCatch = require('safety-catch')
const RandomIterator = require('random-array-iterator')
const RemoteBitfield = require('./remote-bitfield')
const { REQUEST_CANCELLED, REQUEST_TIMEOUT, INVALID_CAPABILITY, SNAPSHOT_NOT_AVAILABLE } = require('./errors')
const m = require('./messages')
const caps = require('./caps')

const DEFAULT_MAX_INFLIGHT = 32
const DEFAULT_SEGMENT_SIZE = 128 * 1024 // 128 KiB

class Attachable {
  constructor () {
    this.resolved = false
    this.refs = []
  }

  attach (session) {
    const r = {
      context: this,
      session,
      sindex: 0,
      rindex: 0,
      snapshot: true,
      resolve: null,
      reject: null,
      promise: null,
      timeout: null
    }

    r.sindex = session.push(r) - 1
    r.rindex = this.refs.push(r) - 1
    r.promise = new Promise((resolve, reject) => {
      r.resolve = resolve
      r.reject = reject
    })

    return r
  }

  detach (r, err = null) {
    if (r.context !== this) return false

    this._detach(r)
    this._cancel(r, err)
    this.gc()

    return true
  }

  _detach (r) {
    const rh = this.refs.pop()
    const sh = r.session.pop()

    if (r.rindex < this.refs.length) this.refs[rh.rindex = r.rindex] = rh
    if (r.sindex < r.session.length) r.session[sh.sindex = r.sindex] = sh

    destroyRequestTimeout(r)
    r.context = null

    return r
  }

  gc () {
    if (this.refs.length === 0) this._unref()
  }

  _cancel (r, err) {
    r.reject(err || REQUEST_CANCELLED())
  }

  _unref () {
    // overwrite me
  }

  resolve (val) {
    this.resolved = true
    while (this.refs.length > 0) {
      this._detach(this.refs[this.refs.length - 1]).resolve(val)
    }
  }

  reject (err) {
    this.resolved = true
    while (this.refs.length > 0) {
      this._detach(this.refs[this.refs.length - 1]).reject(err)
    }
  }

  setTimeout (r, ms) {
    destroyRequestTimeout(r)
    r.timeout = setTimeout(onrequesttimeout, ms, r)
  }
}

class BlockRequest extends Attachable {
  constructor (tracker, index) {
    super()

    this.index = index
    this.inflight = []
    this.queued = false
    this.tracker = tracker
  }

  _unref () {
    if (this.inflight.length > 0) return
    this.tracker.remove(this.index)
  }
}

class RangeRequest extends Attachable {
  constructor (ranges, start, end, linear, ifAvailable, blocks) {
    super()

    this.start = start
    this.end = end
    this.linear = linear
    this.ifAvailable = ifAvailable
    this.blocks = blocks
    this.ranges = ranges

    // As passed by the user, immut
    this.userStart = start
    this.userEnd = end
  }

  _unref () {
    const i = this.ranges.indexOf(this)
    if (i === -1) return
    const h = this.ranges.pop()
    if (i < this.ranges.length - 1) this.ranges[i] = h
  }

  _cancel (r) {
    r.resolve(false)
  }
}

class UpgradeRequest extends Attachable {
  constructor (replicator, fork, length) {
    super()

    this.fork = fork
    this.length = length
    this.inflight = []
    this.replicator = replicator
  }

  _unref () {
    if (this.replicator.eagerUpgrade === true || this.inflight.length > 0) return
    this.replicator._upgrade = null
  }

  _cancel (r) {
    r.resolve(false)
  }
}

class SeekRequest extends Attachable {
  constructor (seeks, seeker) {
    super()

    this.seeker = seeker
    this.inflight = []
    this.seeks = seeks
  }

  _unref () {
    if (this.inflight.length > 0) return
    const i = this.seeks.indexOf(this)
    if (i === -1) return
    const h = this.seeks.pop()
    if (i < this.seeks.length - 1) this.seeks[i] = h
  }
}

class InflightTracker {
  constructor () {
    this._requests = []
    this._free = []
  }

  get idle () {
    return this._requests.length === this._free.length
  }

  * [Symbol.iterator] () {
    for (const req of this._requests) {
      if (req !== null) yield req
    }
  }

  add (req) {
    const id = this._free.length ? this._free.pop() : this._requests.push(null)

    req.id = id
    this._requests[id - 1] = req
    return req
  }

  get (id) {
    return id <= this._requests.length ? this._requests[id - 1] : null
  }

  remove (id) {
    if (id <= this._requests.length) {
      this._requests[id - 1] = null
      this._free.push(id)
    }
  }
}

class BlockTracker {
  constructor () {
    this._map = new Map()
  }

  [Symbol.iterator] () {
    return this._map.values()
  }

  isEmpty () {
    return this._map.size === 0
  }

  has (index) {
    return this._map.has(index)
  }

  get (index) {
    return this._map.get(index) || null
  }

  add (index) {
    let b = this._map.get(index)
    if (b) return b

    b = new BlockRequest(this, index)
    this._map.set(index, b)

    return b
  }

  remove (index) {
    const b = this.get(index)
    this._map.delete(index)
    return b
  }
}

class Peer {
  constructor (replicator, protomux, channel, session) {
    this.core = replicator.core
    this.replicator = replicator
    this.stream = protomux.stream
    this.protomux = protomux
    this.remotePublicKey = this.stream.remotePublicKey

    this.paused = false

    this.session = session

    this.channel = channel
    this.channel.userData = this

    this.wireSync = this.channel.messages[0]
    this.wireRequest = this.channel.messages[1]
    this.wireCancel = null
    this.wireData = this.channel.messages[3]
    this.wireNoData = this.channel.messages[4]
    this.wireWant = this.channel.messages[5]
    this.wireUnwant = this.channel.messages[6]
    this.wireBitfield = this.channel.messages[7]
    this.wireRange = this.channel.messages[8]
    this.wireExtension = this.channel.messages[9]

    this.inflight = 0
    this.maxInflight = DEFAULT_MAX_INFLIGHT
    this.dataProcessing = 0

    this.canUpgrade = true

    this.needsSync = false
    this.syncsProcessing = 0

    // TODO: tweak pipelining so that data sent BEFORE remoteOpened is not cap verified!
    // we might wanna tweak that with some crypto, ie use the cap to encrypt it...
    // or just be aware of that, to only push non leaky data

    this.remoteOpened = false
    this.remoteBitfield = new RemoteBitfield()

    this.remoteFork = 0
    this.remoteLength = 0
    this.remoteCanUpgrade = false
    this.remoteUploading = true
    this.remoteDownloading = true
    this.remoteSynced = false

    this.segmentsWanted = new Set()
    this.broadcastedNonSparse = false

    this.lengthAcked = 0

    this.extensions = new Map()
    this.lastExtensionSent = ''
    this.lastExtensionRecv = ''

    replicator._ifAvailable++
  }

  signalUpgrade () {
    if (this._shouldUpdateCanUpgrade() === true) this._updateCanUpgradeAndSync()
    else this.sendSync()
  }

  broadcastRange (start, length, drop) {
    this.wireRange.send({
      drop,
      start,
      length
    })
  }

  extension (name, message) {
    this.wireExtension.send({ name: name === this.lastExtensionSent ? '' : name, message })
    this.lastExtensionSent = name
  }

  onextension (message) {
    const name = message.name || this.lastExtensionRecv
    this.lastExtensionRecv = name
    const ext = this.extensions.get(name)
    if (ext) ext._onmessage({ start: 0, end: message.byteLength, buffer: message.message }, this)
  }

  sendSync () {
    if (this.syncsProcessing !== 0) {
      this.needsSync = true
      return
    }

    if (this.core.tree.fork !== this.remoteFork) {
      this.canUpgrade = false
    }

    this.needsSync = false

    this.wireSync.send({
      fork: this.core.tree.fork,
      length: this.core.tree.length,
      remoteLength: this.core.tree.fork === this.remoteFork ? this.remoteLength : 0,
      canUpgrade: this.canUpgrade,
      uploading: true,
      downloading: true
    })
  }

  onopen ({ capability }) {
    const expected = caps.replicate(this.stream.isInitiator === false, this.replicator.key, this.stream.handshakeHash)

    if (b4a.equals(capability, expected) !== true) { // TODO: change this to a rejection instead, less leakage
      throw INVALID_CAPABILITY('Remote sent an invalid replication capability')
    }

    if (this.remoteOpened === true) return
    this.remoteOpened = true

    this.protomux.cork()

    this.sendSync()

    const contig = this.core.header.contiguousLength
    if (contig > 0) {
      this.broadcastRange(0, contig, false)

      if (contig === this.core.tree.length) {
        this.broadcastedNonSparse = true
      }
    }

    this.replicator._ifAvailable--
    this.replicator._addPeer(this)

    this.protomux.uncork()
  }

  onclose (isRemote) {
    if (this.session) this.session.close().catch(noop)

    if (this.remoteOpened === false) {
      this.replicator._ifAvailable--
      this.replicator.updateAll()
      return
    }

    this.remoteOpened = false
    this.replicator._removePeer(this)
  }

  async onsync ({ fork, length, remoteLength, canUpgrade, uploading, downloading }) {
    const lengthChanged = length !== this.remoteLength
    const sameFork = fork === this.core.tree.fork

    this.remoteSynced = true
    this.remoteFork = fork
    this.remoteLength = length
    this.remoteCanUpgrade = canUpgrade
    this.remoteUploading = uploading
    this.remoteDownloading = downloading

    this.lengthAcked = sameFork ? remoteLength : 0
    this.syncsProcessing++

    this.replicator._updateFork(this)

    if (this.remoteLength > this.core.tree.length && this.lengthAcked === this.core.tree.length) {
      if (this.replicator._addUpgradeMaybe() !== null) this._update()
    }

    const upgrade = (lengthChanged === false || sameFork === false)
      ? this.canUpgrade && sameFork
      : await this._canUpgrade(length, fork)

    if (length === this.remoteLength && fork === this.core.tree.fork) {
      this.canUpgrade = upgrade
    }

    if (--this.syncsProcessing !== 0) return // ie not latest

    if (this.needsSync === true || (this.core.tree.fork === this.remoteFork && this.core.tree.length > this.remoteLength)) {
      this.signalUpgrade()
    }

    this._update()
  }

  _shouldUpdateCanUpgrade () {
    return this.core.tree.fork === this.remoteFork &&
      this.core.tree.length > this.remoteLength &&
      this.canUpgrade === false &&
      this.syncsProcessing === 0
  }

  async _updateCanUpgradeAndSync () {
    const { length, fork } = this.core.tree

    const canUpgrade = await this._canUpgrade(this.remoteLength, this.remoteFork)

    if (this.syncsProcessing > 0 || length !== this.core.tree.length || fork !== this.core.tree.fork) {
      return
    }
    if (canUpgrade === this.canUpgrade) {
      return
    }

    this.canUpgrade = canUpgrade
    this.sendSync()
  }

  // Safe to call in the background - never fails
  async _canUpgrade (remoteLength, remoteFork) {
    if (remoteFork !== this.core.tree.fork) return false

    if (remoteLength === 0) return true
    if (remoteLength >= this.core.tree.length) return false

    try {
      // Rely on caching to make sure this is cheap...
      const canUpgrade = await this.core.tree.upgradeable(remoteLength)

      if (remoteFork !== this.core.tree.fork) return false

      return canUpgrade
    } catch {
      return false
    }
  }

  async _getProof (msg) {
    const proof = await this.core.tree.proof(msg)

    if (proof.block) {
      const index = msg.block.index

      if (msg.fork !== this.core.tree.fork || !this.core.bitfield.get(index)) {
        return null
      }

      proof.block.value = await this.core.blocks.get(index)
    }

    return proof
  }

  async onrequest (msg) {
    let proof = null

    // TODO: could still be answerable if (index, fork) is an ancestor of the current fork
    if (msg.fork === this.core.tree.fork) {
      try {
        proof = await this._getProof(msg)
      } catch (err) { // TODO: better error handling here, ie custom errors
        safetyCatch(err)
      }
    }

    if (proof !== null) {
      if (proof.block !== null) {
        this.replicator.onupload(proof.block.index, proof.block.value, this)
      }

      this.wireData.send({
        request: msg.id,
        fork: msg.fork,
        block: proof.block,
        hash: proof.hash,
        seek: proof.seek,
        upgrade: proof.upgrade
      })
      return
    }

    this.wireNoData.send({
      request: msg.id
    })
  }

  _checkIfConflict (err) {
    this.paused = true

    const length = Math.min(this.core.tree.length, this.remoteLength)
    if (length === 0) throw err

    this.wireRequest.send({
      id: 0, // TODO: use an more explicit id for this eventually...
      fork: this.remoteFork,
      block: null,
      hash: null,
      seek: null,
      upgrade: {
        start: 0,
        length
      }
    })
  }

  async ondata (data) {
    // always allow a fork conflict proof to be sent
    if (data.request === 0 && data.upgrade && data.upgrade.start === 0) {
      if (await this.core.checkConflict(data, this)) return
      this.paused = false
    }

    const req = data.request > 0 ? this.replicator._inflight.get(data.request) : null
    const reorg = data.fork > this.core.tree.fork

    // no push atm, TODO: check if this satisfies another pending request
    // allow reorg pushes tho as those are not written to storage so we'll take all the help we can get
    if (req === null && reorg === false) return

    if (req !== null) {
      if (req.peer !== this) return
      this.inflight--
      this.replicator._inflight.remove(req.id)
    }

    if (reorg === true) return this.replicator._onreorgdata(this, req, data)

    this.dataProcessing++

    try {
      if (!matchingRequest(req, data) || !(await this.core.verify(data, this))) {
        this.replicator._onnodata(this, req)
        return
      }
    } catch (err) {
      safetyCatch(err)
      // might be a fork, verify
      this._checkIfConflict(err)
      this.replicator._onnodata(this, req)
      return
    } finally {
      this.dataProcessing--
    }

    this.replicator._ondata(this, req, data)

    if (this._shouldUpdateCanUpgrade() === true) {
      this._updateCanUpgradeAndSync()
    }
  }

  onnodata ({ request }) {
    const req = request > 0 ? this.replicator._inflight.get(request) : null

    if (req === null || req.peer !== this) return

    this.inflight--
    this.replicator._inflight.remove(req.id)
    this.replicator._onnodata(this, req)
  }

  onwant ({ start, length }) {
    this.replicator._onwant(this, start, length)
  }

  onunwant () {
    // TODO
  }

  onbitfield ({ start, bitfield }) {
    if (this.remoteBitfield.insert(start, bitfield)) {
      this._update()
    }
  }

  onrange ({ drop, start, length }) {
    const has = drop === false

    this.remoteBitfield.setRange(start, length, has)

    if (drop === false) this._update()
  }

  onreorghint () {
    // TODO
  }

  _update () {
    // TODO: if this is in a batch or similar it would be better to defer it
    // we could do that with nextTick/microtick mb? (combined with a property on the session to signal read buffer mb)
    this.replicator.updatePeer(this)
  }

  async _onconflict () {
    this.protomux.cork()
    if (this.remoteLength > 0 && this.core.tree.fork === this.remoteFork) {
      await this.onrequest({
        id: 0,
        fork: this.core.tree.fork,
        block: null,
        hash: null,
        seek: null,
        upgrade: {
          start: 0,
          length: Math.min(this.core.tree.length, this.remoteLength)
        }
      })
    }
    this.channel.close()
    this.protomux.uncork()
  }

  _makeRequest (needsUpgrade) {
    if (needsUpgrade === true && this.replicator._shouldUpgrade(this) === false) {
      return null
    }

    if (needsUpgrade === false && this.replicator._autoUpgrade(this) === true) {
      needsUpgrade = true
    }

    return {
      peer: this,
      id: 0,
      fork: this.remoteFork,
      block: null,
      hash: null,
      seek: null,
      upgrade: needsUpgrade === false
        ? null
        : { start: this.core.tree.length, length: this.remoteLength - this.core.tree.length }
    }
  }

  _requestUpgrade (u) {
    const req = this._makeRequest(true)
    if (req === null) return false

    this._send(req)

    return true
  }

  _requestSeek (s) {
    const { length, fork } = this.core.tree

    if (fork !== this.remoteFork) return false

    if (s.seeker.start >= length) {
      const req = this._makeRequest(true)

      // We need an upgrade for the seek, if non can be provided, skip
      if (req === null) return false

      req.seek = { bytes: s.seeker.bytes }

      s.inflight.push(req)
      this._send(req)

      return true
    }

    const len = s.seeker.end - s.seeker.start
    const off = s.seeker.start + Math.floor(Math.random() * len)

    for (let i = 0; i < len; i++) {
      let index = off + i
      if (index > s.seeker.end) index -= len

      if (this.remoteBitfield.get(index) === false) continue
      if (this.core.bitfield.get(index) === true) continue

      // Check if this block is currently inflight - if so pick another
      const b = this.replicator._blocks.get(index)
      if (b !== null && b.inflight.length > 0) continue

      // Block is not inflight, but we only want the hash, check if that is inflight
      const h = this.replicator._hashes.add(index)
      if (h.inflight.length > 0) continue

      const req = this._makeRequest(false)

      req.hash = { index: 2 * index, nodes: 0 }
      req.seek = { bytes: s.seeker.bytes }

      s.inflight.push(req)
      h.inflight.push(req)
      this._send(req)

      return true
    }

    this._maybeWant(s.seeker.start, len)
    return false
  }

  // mb turn this into a YES/NO/MAYBE enum, could simplify ifavail logic
  _blockAvailable (b) { // TODO: fork also
    return this.remoteBitfield.get(b.index)
  }

  _requestBlock (b) {
    const { length, fork } = this.core.tree

    if (this.remoteBitfield.get(b.index) === false || fork !== this.remoteFork) {
      this._maybeWant(b.index)
      return false
    }

    const req = this._makeRequest(b.index >= length)
    if (req === null) return false

    req.block = { index: b.index, nodes: 0 }

    b.inflight.push(req)
    this._send(req)

    return true
  }

  _requestRange (r) {
    const { length, fork } = this.core.tree

    const end = Math.min(r.end === -1 ? this.remoteLength : r.end, this.remoteLength)
    if (end < r.start || fork !== this.remoteFork) return false

    const len = end - r.start
    const off = r.start + (r.linear ? 0 : Math.floor(Math.random() * len))

    // TODO: we should weight this to request blocks < .length first
    // as they are "cheaper" and will trigger an auto upgrade if possible
    // If no blocks < .length is avaible then try the "needs upgrade" range

    for (let i = 0; i < len; i++) {
      let index = off + i
      if (index >= end) index -= len

      if (r.blocks !== null) index = r.blocks[index]

      if (this.remoteBitfield.get(index) === false) continue
      if (this.core.bitfield.get(index) === true) continue

      const b = this.replicator._blocks.add(index)
      if (b.inflight.length > 0) continue

      const req = this._makeRequest(index >= length)

      // If the request cannot be satisfied, dealloc the block request if no one is subscribed to it
      if (req === null) {
        b.gc()
        return false
      }

      req.block = { index, nodes: 0 }

      b.inflight.push(req)
      this._send(req)

      // Don't think this will ever happen, as the pending queue is drained before the range queue
      // but doesn't hurt to check this explicitly here also.
      if (b.queued) b.queued = false

      return true
    }

    this._maybeWant(r.start, len)
    return false
  }

  _requestForkProof (f) {
    const req = this._makeRequest(false)

    req.upgrade = { start: 0, length: this.remoteLength }

    f.inflight.push(req)
    this._send(req)
  }

  _requestForkRange (f) {
    if (f.fork !== this.remoteFork || f.batch.want === null) return false

    const end = Math.min(f.batch.want.end, this.remoteLength)
    if (end < f.batch.want.start) return false

    const len = end - f.batch.want.start
    const off = f.batch.want.start + Math.floor(Math.random() * len)

    for (let i = 0; i < len; i++) {
      let index = off + i
      if (index >= end) index -= len

      if (this.remoteBitfield.get(index) === false) continue

      const req = this._makeRequest(false)

      req.hash = { index: 2 * index, nodes: f.batch.want.nodes }

      f.inflight.push(req)
      this._send(req)

      return true
    }

    this._maybeWant(f.batch.want.start, len)
    return false
  }

  _maybeWant (start, length = 1) {
    let i = Math.floor(start / DEFAULT_SEGMENT_SIZE)
    const n = Math.ceil((start + length) / DEFAULT_SEGMENT_SIZE)

    for (; i < n; i++) {
      if (this.segmentsWanted.has(i)) continue
      this.segmentsWanted.add(i)

      this.wireWant.send({
        start: i * DEFAULT_SEGMENT_SIZE,
        length: DEFAULT_SEGMENT_SIZE
      })
    }
  }

  async _send (req) {
    const fork = this.core.tree.fork

    this.inflight++
    this.replicator._inflight.add(req)

    if (req.upgrade !== null && req.fork === fork) {
      const u = this.replicator._addUpgrade()
      u.inflight.push(req)
    }

    try {
      if (req.block !== null && req.fork === fork) {
        req.block.nodes = await this.core.tree.missingNodes(2 * req.block.index)
      }
      if (req.hash !== null && req.fork === fork) {
        req.hash.nodes = await this.core.tree.missingNodes(req.hash.index)
      }
    } catch (err) {
      this.stream.destroy(err)
      return
    }

    this.wireRequest.send(req)
  }
}

module.exports = class Replicator {
  constructor (core, key, { eagerUpgrade = true, allowFork = true, onpeerupdate = noop, onupload = noop } = {}) {
    this.key = key
    this.discoveryKey = core.crypto.discoveryKey(key)
    this.core = core
    this.eagerUpgrade = eagerUpgrade
    this.allowFork = allowFork
    this.onpeerupdate = onpeerupdate
    this.onupload = onupload
    this.peers = []
    this.findingPeers = 0 // updateable from the outside

    this._inflight = new InflightTracker()
    this._blocks = new BlockTracker()
    this._hashes = new BlockTracker()

    this._queued = []

    this._seeks = []
    this._upgrade = null
    this._reorgs = []
    this._ranges = []

    this._hadPeers = false
    this._ifAvailable = 0
    this._updatesPending = 0
    this._applyingReorg = null
  }

  cork () {
    for (const peer of this.peers) peer.protomux.cork()
  }

  uncork () {
    for (const peer of this.peers) peer.protomux.uncork()
  }

  // Called externally when a range of new blocks has been processed/removed
  onhave (start, length, drop = false) {
    for (const peer of this.peers) peer.broadcastRange(start, length, drop)
  }

  // Called externally when a truncation upgrade has been processed
  ontruncate (newLength) {
    const notify = []

    for (const blk of this._blocks) {
      if (blk.index < newLength) continue
      notify.push(blk)
    }

    for (const blk of notify) {
      for (const r of blk.refs) {
        if (r.snapshot === false) continue
        blk.detach(r, SNAPSHOT_NOT_AVAILABLE())
      }
    }
  }

  // Called externally when a upgrade has been processed
  onupgrade () {
    for (const peer of this.peers) peer.signalUpgrade()
    if (this._blocks.isEmpty() === false) this._resolveBlocksLocally()
    if (this._upgrade !== null) this._resolveUpgradeRequest(null)
    if (this._ranges.length !== 0 || this._seeks.length !== 0) this._updateNonPrimary()
  }

  // Called externally when a conflict has been detected and verified
  async onconflict (from) {
    const all = []
    for (const peer of this.peers) {
      all.push(peer._onconflict())
    }
    await Promise.allSettled(all)
  }

  async applyPendingReorg () {
    if (this._applyingReorg !== null) {
      await this._applyingReorg
      return true
    }

    for (let i = this._reorgs.length - 1; i >= 0; i--) {
      const f = this._reorgs[i]
      if (f.batch !== null && f.batch.finished) {
        await this._applyReorg(f)
        return true
      }
    }

    return false
  }

  addUpgrade (session) {
    if (this._upgrade !== null) {
      const ref = this._upgrade.attach(session)
      this._checkUpgradeIfAvailable()
      return ref
    }

    const ref = this._addUpgrade().attach(session)

    this.updateAll()

    return ref
  }

  addBlock (session, index) {
    const b = this._blocks.add(index)
    const ref = b.attach(session)

    this._queueBlock(b)
    this.updateAll()

    return ref
  }

  addSeek (session, seeker) {
    const s = new SeekRequest(this._seeks, seeker)
    const ref = s.attach(session)

    this._seeks.push(s)
    this.updateAll()

    return ref
  }

  addRange (session, { start = 0, end = -1, length = toLength(start, end), blocks = null, linear = false, ifAvailable = false } = {}) {
    if (blocks !== null) { // if using blocks, start, end just acts as frames around the blocks array
      start = 0
      end = length = blocks.length
    }

    const r = new RangeRequest(
      this._ranges,
      start,
      length === -1 ? -1 : start + length,
      linear,
      ifAvailable,
      blocks
    )

    const ref = r.attach(session)

    this._ranges.push(r)

    // Trigger this to see if this is already resolved...
    // Also auto compresses the range based on local bitfield
    this._updateNonPrimary()

    return ref
  }

  cancel (ref) {
    ref.context.detach(ref, null)
  }

  clearRequests (session, err = null) {
    while (session.length > 0) {
      const ref = session[session.length - 1]
      ref.context.detach(ref, err)
    }

    this.updateAll()
  }

  _addUpgradeMaybe () {
    return this.eagerUpgrade === true ? this._addUpgrade() : this._upgrade
  }

  // TODO: this function is OVER called atm, at each updatePeer/updateAll
  // instead its more efficient to only call it when the conditions in here change - ie on sync/add/remove peer
  // Do this when we have more tests.
  _checkUpgradeIfAvailable () {
    if (this._ifAvailable > 0 || this._upgrade === null || this._upgrade.refs.length === 0) return
    if (this._hadPeers === false && this.findingPeers > 0) return

    // check if a peer can upgrade us

    for (let i = 0; i < this.peers.length; i++) {
      const peer = this.peers[i]

      if (peer.remoteSynced === false) return

      if (this.core.tree.length === 0 && peer.remoteLength > 0) return

      if (peer.remoteLength <= this._upgrade.length || peer.remoteFork !== this._upgrade.fork) continue

      if (peer.syncsProcessing > 0) return

      if (peer.lengthAcked !== this.core.tree.length && peer.remoteFork === this.core.tree.fork) return
      if (peer.remoteCanUpgrade === true) return
    }

    // check if reorgs in progress...

    if (this._applyingReorg !== null) return

    // TODO: we prob should NOT wait for inflight reorgs here, seems better to just resolve the upgrade
    // and then apply the reorg on the next call in case it's slow - needs some testing in practice

    for (let i = 0; i < this._reorgs.length; i++) {
      const r = this._reorgs[i]
      if (r.inflight.length > 0) return
    }

    // nothing to do, indicate no update avail

    const u = this._upgrade
    this._upgrade = null
    u.resolve(false)
  }

  _addUpgrade () {
    if (this._upgrade !== null) return this._upgrade

    // TODO: needs a reorg: true/false flag to indicate if the user requested a reorg
    this._upgrade = new UpgradeRequest(this, this.core.tree.fork, this.core.tree.length)

    return this._upgrade
  }

  _addReorg (fork, peer) {
    if (this.allowFork === false) return null

    // TODO: eager gc old reorgs from the same peer
    // not super important because they'll get gc'ed when the request finishes
    // but just spam the remote can do ...

    for (const f of this._reorgs) {
      if (f.fork > fork && f.batch !== null) return null
      if (f.fork === fork) return f
    }

    const f = {
      fork,
      inflight: [],
      batch: null
    }

    this._reorgs.push(f)

    // maintain sorted by fork
    let i = this._reorgs.length - 1
    while (i > 0 && this._reorgs[i - 1].fork > fork) {
      this._reorgs[i] = this._reorgs[i - 1]
      this._reorgs[--i] = f
    }

    return f
  }

  _shouldUpgrade (peer) {
    if (this._upgrade !== null && this._upgrade.inflight.length > 0) return false
    return peer.remoteCanUpgrade === true &&
      peer.remoteLength > this.core.tree.length &&
      peer.lengthAcked === this.core.tree.length
  }

  _autoUpgrade (peer) {
    return this._upgrade !== null && peer.remoteFork === this.core.tree.fork && this._shouldUpgrade(peer)
  }

  _addPeer (peer) {
    this._hadPeers = true
    this.peers.push(peer)
    this.updatePeer(peer)
    this.onpeerupdate(true, peer)
  }

  _removePeer (peer) {
    this.peers.splice(this.peers.indexOf(peer), 1)

    for (const req of this._inflight) {
      if (req.peer !== peer) continue
      this._inflight.remove(req.id)
      this._clearRequest(peer, req)
    }

    this.onpeerupdate(false, peer)
    this.updateAll()
  }

  _queueBlock (b) {
    if (b.inflight.length > 0 || b.queued === true) return
    b.queued = true
    this._queued.push(b)
  }

  // Runs in the background - not allowed to throw
  async _resolveBlocksLocally () {
    // TODO: check if fork compat etc. Requires that we pass down truncation info

    let clear = null

    for (const b of this._blocks) {
      if (this.core.bitfield.get(b.index) === false) continue

      try {
        b.resolve(await this.core.blocks.get(b.index))
      } catch (err) {
        b.reject(err)
      }

      if (clear === null) clear = []
      clear.push(b)
    }

    if (clear === null) return

    // Currently the block tracker does not support deletes during iteration, so we make
    // sure to clear them afterwards.
    for (const b of clear) {
      this._blocks.remove(b.index)
    }
  }

  _resolveBlockRequest (tracker, index, value, req) {
    const b = tracker.remove(index)
    if (b === null) return false

    removeInflight(b.inflight, req)
    b.queued = false

    b.resolve(value)

    return true
  }

  _resolveUpgradeRequest (req) {
    if (req !== null) removeInflight(this._upgrade.inflight, req)

    if (this.core.tree.length === this._upgrade.length && this.core.tree.fork === this._upgrade.fork) return false

    const u = this._upgrade
    this._upgrade = null
    u.resolve(true)

    return true
  }

  _resolveRangeRequest (req, index) {
    const head = this._ranges.pop()

    if (index < this._ranges.length) this._ranges[index] = head

    req.resolve(true)
  }

  _clearInflightBlock (tracker, req) {
    const isBlock = tracker === this._blocks
    const index = isBlock === true ? req.block.index : req.hash.index / 2
    const b = tracker.get(index)

    if (b === null || removeInflight(b.inflight, req) === false) return

    if (b.refs.length > 0 && isBlock === true) {
      this._queueBlock(b)
      return
    }

    b.gc()
  }

  _clearInflightUpgrade (req) {
    if (removeInflight(this._upgrade.inflight, req) === false) return
    this._upgrade.gc()
  }

  _clearInflightSeeks (req) {
    for (const s of this._seeks) {
      if (removeInflight(s.inflight, req) === false) continue
      s.gc()
    }
  }

  _clearInflightReorgs (req) {
    for (const r of this._reorgs) {
      removeInflight(r.inflight, req)
    }
  }

  _clearOldReorgs (fork) {
    for (let i = 0; i < this._reorgs.length; i++) {
      const f = this._reorgs[i]
      if (f.fork >= fork) continue
      if (i === this._reorgs.length - 1) this._reorgs.pop()
      else this._reorgs[i] = this._reorgs.pop()
      i--
    }
  }

  // "slow" updates here - async but not allowed to ever throw
  async _updateNonPrimary () {
    // Check if running, if so skip it and the running one will issue another update for us (debounce)
    while (++this._updatesPending === 1) {
      for (let i = 0; i < this._ranges.length; i++) {
        const r = this._ranges[i]

        clampRange(this.core, r)

        if (r.end !== -1 && r.start >= r.end) {
          this._resolveRangeRequest(r, i--)
        }
      }

      for (let i = 0; i < this._seeks.length; i++) {
        const s = this._seeks[i]

        let err = null
        let res = null

        try {
          res = await s.seeker.update()
        } catch (error) {
          err = error
        }

        if (!res && !err) continue

        if (i < this._seeks.length - 1) this._seeks[i] = this._seeks.pop()
        else this._seeks.pop()

        i--

        if (err) s.reject(err)
        else s.resolve(res)
      }

      if (this._inflight.idle) this.updateAll()

      // No additional updates scheduled - return
      if (--this._updatesPending === 0) return
      // Debounce the additional updates - continue
      this._updatesPending = 0
    }
  }

  _maybeResolveIfAvailableRanges () {
    if (this._ifAvailable > 0 || !this._inflight.idle || !this._ranges.length) return

    for (let i = 0; i < this.peers.length; i++) {
      if (this.peers[i].dataProcessing > 0) return
    }

    for (let i = 0; i < this._ranges.length; i++) {
      const r = this._ranges[i]

      if (r.ifAvailable) {
        this._resolveRangeRequest(r, i--)
      }
    }
  }

  _clearRequest (peer, req) {
    if (req.block !== null) {
      this._clearInflightBlock(this._blocks, req)
    }

    if (req.hash !== null) {
      this._clearInflightBlock(this._hashes, req)
    }

    if (req.upgrade !== null && this._upgrade !== null) {
      this._clearInflightUpgrade(req)
    }

    if (this._seeks.length > 0) {
      this._clearInflightSeeks(req)
    }

    if (this._reorgs.length > 0) {
      this._clearInflightReorgs(req)
    }
  }

  _onnodata (peer, req) {
    this._clearRequest(peer, req)
    this.updateAll()
  }

  _ondata (peer, req, data) {
    if (data.block !== null) {
      this._resolveBlockRequest(this._blocks, data.block.index, data.block.value, req)
    }

    if (data.hash !== null && (data.hash.index & 1) === 0) {
      this._resolveBlockRequest(this._hashes, data.hash.index / 2, null, req)
    }

    if (this._upgrade !== null) {
      this._resolveUpgradeRequest(req)
    }

    if (this._seeks.length > 0) {
      this._clearInflightSeeks(req)
    }

    if (this._reorgs.length > 0) {
      this._clearInflightReorgs(req)
    }

    if (this._seeks.length > 0 || this._ranges.length > 0) this._updateNonPrimary()
    else this.updatePeer(peer)
  }

  _onwant (peer, start, length) {
    length = Math.min(length, this.core.tree.length - start)

    peer.protomux.cork()

    for (const msg of this.core.bitfield.want(start, length)) {
      peer.wireBitfield.send(msg)
    }

    peer.protomux.uncork()
  }

  async _onreorgdata (peer, req, data) {
    const f = this._addReorg(data.fork, peer)

    if (f === null) {
      this.updateAll()
      return
    }

    removeInflight(f.inflight, req)

    if (f.batch) {
      await f.batch.update(data)
    } else if (data.upgrade) {
      f.batch = await this.core.tree.reorg(data)

      // Remove "older" reorgs in progress as we just verified this one.
      this._clearOldReorgs(f.fork)
    }

    if (f.batch && f.batch.finished) {
      if (this._addUpgradeMaybe() !== null) {
        await this._applyReorg(f)
      }
    }

    this.updateAll()
  }

  // Never throws, allowed to run in the background
  async _applyReorg (f) {
    // TODO: more optimal here to check if potentially a better reorg
    // is available, ie higher fork, and request that one first.
    // This will request that one after this finishes, which is fine, but we
    // should investigate the complexity in going the other way

    const u = this._upgrade

    this._reorgs = [] // clear all as the nodes are against the old tree - easier
    this._applyingReorg = this.core.reorg(f.batch, null) // TODO: null should be the first/last peer?

    try {
      await this._applyingReorg
    } catch (err) {
      this._upgrade = null
      u.reject(err)
    }

    this._applyingReorg = null

    if (this._upgrade !== null) {
      this._resolveUpgradeRequest(null)
    }

    for (const peer of this.peers) this._updateFork(peer)

    // TODO: all the remaining is a tmp workaround until we have a flag/way for ANY_FORK
    for (const r of this._ranges) {
      r.start = r.userStart
      r.end = r.userEnd
    }

    this.updateAll()
  }

  _maybeUpdate () {
    return this._upgrade !== null && this._upgrade.inflight.length === 0
  }

  _updateFork (peer) {
    if (this._applyingReorg !== null || this.allowFork === false || peer.remoteFork <= this.core.tree.fork) {
      return false
    }

    const f = this._addReorg(peer.remoteFork, peer)

    // TODO: one per peer is better
    if (f !== null && f.batch === null && f.inflight.length === 0) {
      return peer._requestForkProof(f)
    }

    return false
  }

  _updatePeer (peer) {
    if (peer.paused || peer.inflight >= peer.maxInflight) {
      return false
    }

    for (const s of this._seeks) {
      if (s.inflight.length > 0) continue // TODO: one per peer is better
      if (peer._requestSeek(s) === true) {
        return true
      }
    }

    // Implied that any block in the queue should be requested, no matter how many inflights
    const blks = new RandomIterator(this._queued)

    for (const b of blks) {
      if (b.queued === false || peer._requestBlock(b) === true) {
        b.queued = false
        blks.dequeue()
        return true
      }
    }

    return false
  }

  _updatePeerNonPrimary (peer) {
    if (peer.paused || peer.inflight >= peer.maxInflight) {
      return false
    }

    const ranges = new RandomIterator(this._ranges)

    for (const r of ranges) {
      if (peer._requestRange(r) === true) {
        return true
      }
    }

    // Iterate from newest fork to oldest fork...
    for (let i = this._reorgs.length - 1; i >= 0; i--) {
      const f = this._reorgs[i]
      if (f.batch !== null && f.inflight.length === 0 && peer._requestForkRange(f) === true) {
        return true
      }
    }

    if (this._maybeUpdate() === true && peer._requestUpgrade(this._upgrade) === true) {
      return true
    }

    return false
  }

  updatePeer (peer) {
    // Quick shortcut to wait for flushing reorgs - not needed but less waisted requests
    if (this._applyingReorg !== null) return

    while (this._updatePeer(peer) === true);
    while (this._updatePeerNonPrimary(peer) === true);

    this._checkUpgradeIfAvailable()
    this._maybeResolveIfAvailableRanges()
  }

  updateAll () {
    // Quick shortcut to wait for flushing reorgs - not needed but less waisted requests
    if (this._applyingReorg !== null) return

    const peers = new RandomIterator(this.peers)

    for (const peer of peers) {
      if (this._updatePeer(peer) === true) {
        peers.requeue()
      }
    }

    // Check if we can skip the non primary check fully
    if (this._maybeUpdate() === false && this._ranges.length === 0 && this._reorgs.length === 0) {
      this._checkUpgradeIfAvailable()
      return
    }

    for (const peer of peers.restart()) {
      if (this._updatePeerNonPrimary(peer) === true) {
        peers.requeue()
      }
    }

    this._checkUpgradeIfAvailable()
    this._maybeResolveIfAvailableRanges()
  }

  attachTo (protomux, session) {
    const makePeer = this._makePeer.bind(this, protomux, session)

    protomux.pair({ protocol: 'hypercore/alpha', id: this.discoveryKey }, makePeer)
    this._ifAvailable++
    protomux.stream.opened.then((opened) => {
      this._ifAvailable--
      if (opened) makePeer()
      else if (session) session.close().catch(noop)
      this._checkUpgradeIfAvailable()
    })
  }

  destroy () {
    for (const peer of this.peers) {
      peer.protomux.unpair({ protocol: 'hypercore/alpha', id: this.discoveryKey })
      peer.channel.close()
    }
  }

  _makePeer (protomux, session) {
    if (protomux.opened({ protocol: 'hypercore/alpha', id: this.discoveryKey })) return onnochannel()

    const channel = protomux.createChannel({
      userData: null,
      protocol: 'hypercore/alpha',
      aliases: ['hypercore'],
      id: this.discoveryKey,
      handshake: m.wire.handshake,
      messages: [
        { encoding: m.wire.sync, onmessage: onwiresync },
        { encoding: m.wire.request, onmessage: onwirerequest },
        null, // oncancel
        { encoding: m.wire.data, onmessage: onwiredata },
        { encoding: m.wire.noData, onmessage: onwirenodata },
        { encoding: m.wire.want, onmessage: onwirewant },
        { encoding: m.wire.unwant, onmessage: onwireunwant },
        { encoding: m.wire.bitfield, onmessage: onwirebitfield },
        { encoding: m.wire.range, onmessage: onwirerange },
        { encoding: m.wire.extension, onmessage: onwireextension }
      ],
      onopen: onwireopen,
      onclose: onwireclose
    })

    if (channel === null) return onnochannel()

    const peer = new Peer(this, protomux, channel, session)
    const stream = protomux.stream

    peer.channel.open({
      capability: caps.replicate(stream.isInitiator, this.key, stream.handshakeHash)
    })

    return true

    function onnochannel () {
      if (session) session.close().catch(noop)
      return false
    }
  }
}

function matchingRequest (req, data) {
  if (data.block !== null && (req.block === null || req.block.index !== data.block.index)) return false
  if (data.hash !== null && (req.hash === null || req.hash.index !== data.hash.index)) return false
  if (data.seek !== null && (req.seek === null || req.seek.bytes !== data.seek.bytes)) return false
  if (data.upgrade !== null && req.upgrade === null) return false
  return req.fork === data.fork
}

function removeInflight (inf, req) {
  const i = inf.indexOf(req)
  if (i === -1) return false
  if (i < inf.length - 1) inf[i] = inf.pop()
  else inf.pop()
  return true
}

function noop () {}

function toLength (start, end) {
  return end === -1 ? -1 : (end < start ? 0 : end - start)
}

function clampRange (core, r) {
  if (r.blocks === null) {
    const start = core.bitfield.firstUnset(r.start)

    if (r.end === -1) {
      r.start = start === -1 ? core.tree.length : start
    } else {
      const end = core.bitfield.lastUnset(r.end - 1) + 1

      if (start === -1) {
        r.start = r.end
      } else {
        r.start = start
        r.end = end
      }
    }
  } else {
    while (r.start < r.end && core.bitfield.get(r.blocks[r.start])) r.start++
    while (r.start < r.end && core.bitfield.get(r.blocks[r.end - 1])) r.end--
  }
}

function onrequesttimeout (req) {
  if (req.context) req.context.detach(req, REQUEST_TIMEOUT())
}

function destroyRequestTimeout (req) {
  if (req.timeout !== null) {
    clearTimeout(req.timeout)
    req.timeout = null
  }
}

function onwireopen (m, c) {
  return c.userData.onopen(m)
}

function onwireclose (isRemote, c) {
  return c.userData.onclose(isRemote)
}

function onwiresync (m, c) {
  return c.userData.onsync(m)
}

function onwirerequest (m, c) {
  return c.userData.onrequest(m)
}

function onwiredata (m, c) {
  return c.userData.ondata(m)
}

function onwirenodata (m, c) {
  return c.userData.onnodata(m)
}

function onwirewant (m, c) {
  return c.userData.onwant(m)
}

function onwireunwant (m, c) {
  return c.userData.onunwant(m)
}

function onwirebitfield (m, c) {
  return c.userData.onbitfield(m)
}

function onwirerange (m, c) {
  return c.userData.onrange(m)
}

function onwireextension (m, c) {
  return c.userData.onextension(m)
}
