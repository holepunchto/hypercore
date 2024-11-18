/* DEV DOCS
  Every hypercore has one Replicator object managing its connections to other peers.
  There is one Peer object per peer connected to the Hypercore.
  Hypercores do not know about other hypercores, so when a peer is connected to multiple cores, there exists one Peer object per core.

  Hypercore indicates block should be downloaded through methods like Replicator.addRange or Replicator.addBlock
  Hypercore calls Replicator.updateActivity every time a hypercore session opens/closes
  Replicator.updateActivity ensures the Hypercore is downloading blocks as expected
  Replicator keeps track of:
    - Which blocks need to be downloaded (Replicator._blocks)
    - Which blocks currently have inflight requests (Replicator._inflight)

  Blocks are requested from remote peers by Peer objects. The flow is:
    - The replicator's updatePeer method gets called
    - The replicator detects whether the Peer can accept more requests (for example by checking if it's maxed out on inflight blocks)
    - The replicator then tells the Peer what to request (e.g. Peer_requestRange or Peer._requestBlock methods)

  The Peer object is responsible for tracking
    - Which blocks does the Peer have available (tracked in remoteBitfield)
    - Which blocks are you actively looking for from this peer (tracked in missingBlocks)
    - How many blocks are currently inflight (tracked in inflight)
  The Peer uses this information to decide which blocks to request form the peer in response to _requestRange requests and the like.
*/

const b4a = require('b4a')
const safetyCatch = require('safety-catch')
const RandomIterator = require('random-array-iterator')
const flatTree = require('flat-tree')
const ReceiverQueue = require('./receiver-queue')
const HotswapQueue = require('./hotswap-queue')
const RemoteBitfield = require('./remote-bitfield')
const { REQUEST_CANCELLED, REQUEST_TIMEOUT, INVALID_CAPABILITY, SNAPSHOT_NOT_AVAILABLE } = require('hypercore-errors')
const m = require('./messages')
const caps = require('./caps')

const DEFAULT_MAX_INFLIGHT = [16, 512]
const SCALE_LATENCY = 50
const DEFAULT_SEGMENT_SIZE = 256 * 1024 * 8 // 256 KiB in bits
const NOT_DOWNLOADING_SLACK = 20000 + (Math.random() * 20000) | 0
const MAX_PEERS_UPGRADE = 3

const MAX_RANGES = 64

const PRIORITY = {
  NORMAL: 0,
  HIGH: 1,
  VERY_HIGH: 2,
  CANCELLED: 255 // reserved to mark cancellation
}

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
  constructor (tracker, index, priority) {
    super()

    this.index = index
    this.priority = priority
    this.inflight = []
    this.queued = false
    this.hotswap = null
    this.tracker = tracker
  }

  _unref () {
    this.queued = false

    for (const req of this.inflight) {
      req.peer._cancelRequest(req)
    }

    this.tracker.remove(this.index)
    removeHotswap(this)
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
    if (i < this.ranges.length) this.ranges[i] = h
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
    if (i < this.seeks.length) this.seeks[i] = h
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

  remove (id, roundtrip) {
    if (id > this._requests.length) return
    this._requests[id - 1] = null
    if (roundtrip === true) this._free.push(id)
  }

  reusable (id) {
    this._free.push(id)
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

  add (index, priority) {
    let b = this._map.get(index)
    if (b) return b

    b = new BlockRequest(this, index, priority)
    this._map.set(index, b)

    return b
  }

  remove (index) {
    const b = this.get(index)
    this._map.delete(index)
    return b
  }
}

class RoundtripQueue {
  constructor () {
    this.queue = []
    this.tick = 0
  }

  clear () {
    const ids = new Array(this.queue.length)
    for (let i = 0; i < ids.length; i++) {
      ids[i] = this.queue[i][1]
    }

    this.queue = []

    return ids
  }

  add (id) {
    this.queue.push([++this.tick, id])
  }

  flush (tick) {
    let flushed = null

    for (let i = 0; i < this.queue.length; i++) {
      if (this.queue[i][0] > tick) break
      if (flushed === null) flushed = []
      flushed.push(this.queue[i][1])
    }

    if (flushed !== null) this.queue.splice(0, flushed.length)
    return flushed
  }
}

class Peer {
  constructor (replicator, protomux, channel, useSession, inflightRange) {
    this.core = replicator.core
    this.replicator = replicator
    this.stream = protomux.stream
    this.protomux = protomux
    this.remotePublicKey = this.stream.remotePublicKey
    this.remoteSupportsSeeks = false
    this.inflightRange = inflightRange

    this.paused = false
    this.removed = false

    this.useSession = useSession

    this.channel = channel
    this.channel.userData = this

    this.wireSync = this.channel.messages[0]
    this.wireRequest = this.channel.messages[1]
    this.wireCancel = this.channel.messages[2]
    this.wireData = this.channel.messages[3]
    this.wireNoData = this.channel.messages[4]
    this.wireWant = this.channel.messages[5]
    this.wireUnwant = this.channel.messages[6]
    this.wireBitfield = this.channel.messages[7]
    this.wireRange = this.channel.messages[8]
    this.wireExtension = this.channel.messages[9]

    // Same stats as replicator, but for this specific peer
    this.stats = {
      wireSync: { tx: 0, rx: 0 },
      wireRequest: { tx: 0, rx: 0 },
      wireCancel: { tx: 0, rx: 0 },
      wireData: { tx: 0, rx: 0 },
      wireWant: { tx: 0, rx: 0 },
      wireBitfield: { tx: 0, rx: 0 },
      wireRange: { tx: 0, rx: 0 },
      wireExtension: { tx: 0, rx: 0 },
      hotswaps: 0
    }

    this.receiverQueue = new ReceiverQueue()
    this.receiverBusy = false

    // most often not used, so made on demand
    this.roundtripQueue = null

    this.inflight = 0
    this.dataProcessing = 0

    this.canUpgrade = true

    this.needsSync = false
    this.syncsProcessing = 0

    this._remoteContiguousLength = 0

    // TODO: tweak pipelining so that data sent BEFORE remoteOpened is not cap verified!
    // we might wanna tweak that with some crypto, ie use the cap to encrypt it...
    // or just be aware of that, to only push non leaky data

    this.remoteOpened = false
    this.remoteBitfield = new RemoteBitfield()
    this.missingBlocks = new RemoteBitfield()

    this.remoteFork = 0
    this.remoteLength = 0
    this.remoteCanUpgrade = false
    this.remoteUploading = true
    this.remoteDownloading = true
    this.remoteSynced = false
    this.remoteHasManifest = false
    this.remoteRequests = new Map()

    this.segmentsWanted = new Set()
    this.broadcastedNonSparse = false

    this.lengthAcked = 0

    this.extensions = new Map()
    this.lastExtensionSent = ''
    this.lastExtensionRecv = ''

    replicator._ifAvailable++
  }

  get remoteContiguousLength () {
    return this.remoteBitfield.findFirst(false, this._remoteContiguousLength)
  }

  getMaxInflight () {
    const stream = this.stream.rawStream
    if (!stream.udx) return Math.min(this.inflightRange[1], this.inflightRange[0] * 3)

    const scale = stream.rtt <= SCALE_LATENCY ? 1 : stream.rtt / SCALE_LATENCY * Math.min(1, 2 / this.replicator.peers.length)
    return Math.max(this.inflightRange[0], Math.round(Math.min(this.inflightRange[1], this.inflightRange[0] * scale)))
  }

  getMaxHotswapInflight () {
    const inf = this.getMaxInflight()
    return Math.max(16, inf / 2)
  }

  signalUpgrade () {
    if (this._shouldUpdateCanUpgrade() === true) this._updateCanUpgradeAndSync()
    else this.sendSync()
  }

  _markInflight (index) {
    this.missingBlocks.set(index, false)
  }

  broadcastRange (start, length, drop) {
    if (drop) this._unclearLocalRange(start, length)
    else this._clearLocalRange(start, length)

    // TODO: consider also adding early-returns on the drop===true case
    if (!drop) {
      // No need to broadcast if the remote already has this range

      if (this._remoteContiguousLength >= start + length) return

      if (length === 1) {
        if (this.remoteBitfield.get(start)) return
      } else {
        if (this.remoteBitfield.firstUnset(start) >= start + length) return
      }
    }

    this.wireRange.send({
      drop,
      start,
      length
    })
    incrementTx(this.stats.wireRange, this.replicator.stats.wireRange)
  }

  extension (name, message) {
    this.wireExtension.send({ name: name === this.lastExtensionSent ? '' : name, message })
    incrementTx(this.stats.wireExtension, this.replicator.stats.wireExtension)
    this.lastExtensionSent = name
  }

  onextension (message) {
    const name = message.name || this.lastExtensionRecv
    this.lastExtensionRecv = name
    const ext = this.extensions.get(name)
    if (ext) ext._onmessage({ start: 0, end: message.message.byteLength, buffer: message.message }, this)
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
      downloading: this.replicator.isDownloading(),
      hasManifest: !!this.core.header.manifest && this.core.compat === false
    })
    incrementTx(this.stats.wireSync, this.replicator.stats.wireSync)
  }

  onopen ({ seeks, capability }) {
    const expected = caps.replicate(this.stream.isInitiator === false, this.replicator.key, this.stream.handshakeHash)

    if (b4a.equals(capability, expected) !== true) { // TODO: change this to a rejection instead, less leakage
      throw INVALID_CAPABILITY('Remote sent an invalid replication capability')
    }

    if (this.remoteOpened === true) return
    this.remoteOpened = true
    this.remoteSupportsSeeks = seeks

    this.protomux.cork()

    this.sendSync()

    const contig = Math.min(this.core.tree.length, this.core.header.hints.contiguousLength)
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
    // we might have signalled to the remote that we are done (ie not downloading) and the remote might agree on that
    // if that happens, the channel might be closed by the remote. if so just renegotiate it.
    // TODO: add a CLOSE_REASON to mux to we can make this cleaner...
    const reopen = isRemote === true && this.remoteOpened === true && this.remoteDownloading === false &&
       this.remoteUploading === true && this.replicator.downloading === true

    if (this.remoteOpened === false) {
      if (this.useSession) {
        this.replicator._peerSessions--
        this.replicator._closeSessionMaybe()
      }
      this.replicator._ifAvailable--
      this.replicator.updateAll()
      return
    }

    this.remoteOpened = false
    this.removed = true
    this.remoteRequests.clear() // cancel all
    this.receiverQueue.clear()

    if (this.roundtripQueue !== null) {
      for (const id of this.roundtripQueue.clear()) this.replicator._inflight.reusable(id)
    }

    this.replicator._removePeer(this)

    if (reopen) {
      this.replicator._makePeer(this.protomux, this.useSession)
    }

    if (this.useSession) {
      this.replicator._peerSessions--
      this.replicator._closeSessionMaybe()
    }
  }

  closeIfIdle () {
    if (this.remoteDownloading === false && this.replicator.isDownloading() === false) {
      // idling, shut it down...
      this.channel.close()
      return true
    }

    return false
  }

  async onsync ({ fork, length, remoteLength, canUpgrade, uploading, downloading, hasManifest }) {
    const lengthChanged = length !== this.remoteLength
    const sameFork = fork === this.core.tree.fork

    this.remoteSynced = true
    this.remoteFork = fork
    this.remoteLength = length
    this.remoteCanUpgrade = canUpgrade
    this.remoteUploading = uploading
    this.remoteDownloading = downloading
    this.remoteHasManifest = hasManifest

    if (this.closeIfIdle()) return

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

    if (msg.manifest && !this.core.compat) {
      proof.manifest = this.core.header.manifest
    }

    return proof
  }

  async onrequest (msg) {
    const size = this.remoteRequests.size
    this.remoteRequests.set(msg.id, msg)

    // if size didnt change -> id overwrite -> old one is deleted, cancel current and re-add
    if (size === this.remoteRequests.size) {
      this._cancel(msg.id)
      this.remoteRequests.set(msg.id, msg)
    }

    if (!this.protomux.drained || this.receiverQueue.length) {
      this.receiverQueue.push(msg)
      return
    }

    await this._handleRequest(msg)
  }

  oncancel (msg) {
    this._cancel(msg.request)
  }

  _cancel (id) {
    this.remoteRequests.delete(id)
    this.receiverQueue.delete(id)
  }

  ondrain () {
    return this._handleRequests()
  }

  async _handleRequests () {
    if (this.receiverBusy) return
    this.receiverBusy = true
    this.protomux.cork()

    while (this.remoteOpened && this.protomux.drained && this.receiverQueue.length > 0 && !this.removed) {
      const msg = this.receiverQueue.shift()
      await this._handleRequest(msg)
    }

    this.protomux.uncork()
    this.receiverBusy = false
  }

  async _handleRequest (msg) {
    let proof = null

    // TODO: could still be answerable if (index, fork) is an ancestor of the current fork
    if (msg.fork === this.core.tree.fork) {
      try {
        proof = await this._getProof(msg)
      } catch (err) {
        safetyCatch(err)
        if (msg.fork === this.core.tree.fork && isCriticalError(err)) throw err
      }
    }

    // if cancelled do not reply
    if (this.remoteRequests.get(msg.id) !== msg) {
      return
    }

    // sync from now on, so safe to delete from the map
    this.remoteRequests.delete(msg.id)

    if (proof === null) {
      if (msg.manifest && this.core.header.manifest) {
        const manifest = this.core.header.manifest
        this.wireData.send({ request: msg.id, fork: this.core.tree.fork, block: null, hash: null, seek: null, upgrade: null, manifest })
        incrementTx(this.stats.wireData, this.replicator.stats.wireData)
        return
      }

      this.wireNoData.send({ request: msg.id })
      return
    }

    if (proof.block !== null) {
      this.replicator.onupload(proof.block.index, proof.block.value, this)
    }

    this.wireData.send({
      request: msg.id,
      fork: msg.fork,
      block: proof.block,
      hash: proof.hash,
      seek: proof.seek,
      upgrade: proof.upgrade,
      manifest: proof.manifest
    })
    incrementTx(this.stats.wireData, this.replicator.stats.wireData)
  }

  _cancelRequest (req) {
    if (req.priority === PRIORITY.CANCELLED) return
    // mark as cancelled also and avoid re-entry
    req.priority = PRIORITY.CANCELLED

    this.inflight--
    this.replicator._requestDone(req.id, false)

    // clear inflight state
    if (isBlockRequest(req)) this.replicator._unmarkInflight(req.block.index)
    if (isUpgradeRequest(req)) this.replicator._clearInflightUpgrade(req)

    if (this.roundtripQueue === null) this.roundtripQueue = new RoundtripQueue()
    this.roundtripQueue.add(req.id)
    this.wireCancel.send({ request: req.id })
    incrementTx(this.stats.wireCancel, this.replicator.stats.wireCancel)
  }

  _checkIfConflict () {
    this.paused = true

    const length = Math.min(this.core.tree.length, this.remoteLength)
    if (length === 0) return // pause and ignore

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

    incrementTx(this.stats.wireRequest, this.replicator.stats.wireRequest)
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
      this._onrequestroundtrip(req)
    }

    try {
      if (reorg === true) return await this.replicator._onreorgdata(this, req, data)
    } catch (err) {
      safetyCatch(err)
      if (isBlockRequest(req)) this.replicator._unmarkInflight(req.block.index)

      this.paused = true
      this.replicator.oninvalid(err, req, data, this)
      return
    }

    this.dataProcessing++

    try {
      if (!matchingRequest(req, data) || !(await this.core.verify(data, this))) {
        this.replicator._onnodata(this, req)
        return
      }
    } catch (err) {
      safetyCatch(err)
      if (isBlockRequest(req)) this.replicator._unmarkInflight(req.block.index)

      if (err.code === 'WRITE_FAILED') {
        // For example, we don't want to keep pulling data when storage is full
        // TODO: notify the user somehow
        this.paused = true
        return
      }

      if (this.core.closed && !isCriticalError(err)) return

      if (err.code !== 'INVALID_OPERATION') {
        // might be a fork, verify
        this._checkIfConflict()
      }

      this.replicator._onnodata(this, req)
      this.replicator.oninvalid(err, req, data, this)
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

    this._onrequestroundtrip(req)
    this.replicator._onnodata(this, req)
  }

  _onrequestroundtrip (req) {
    if (req.priority === PRIORITY.CANCELLED) return
    // to avoid re-entry we also just mark it as cancelled
    req.priority = PRIORITY.CANCELLED

    this.inflight--
    this.replicator._requestDone(req.id, true)
    if (this.roundtripQueue === null) return
    const flushed = this.roundtripQueue.flush(req.rt)
    if (flushed === null) return
    for (const id of flushed) this.replicator._inflight.reusable(id)
  }

  onwant ({ start, length }) {
    this.replicator._onwant(this, start, length)
  }

  onunwant () {
    // TODO
  }

  onbitfield ({ start, bitfield }) {
    if (start < this._remoteContiguousLength) this._remoteContiguousLength = start // bitfield is always the truth
    this.remoteBitfield.insert(start, bitfield)
    this.missingBlocks.insert(start, bitfield)
    this._clearLocalRange(start, bitfield.byteLength * 8)
    this._update()
  }

  _clearLocalRange (start, length) {
    const bitfield = this.core.skipBitfield === null ? this.core.bitfield : this.core.skipBitfield

    if (length === 1) {
      this.missingBlocks.set(start, this._remoteHasBlock(start) && !bitfield.get(start))
      return
    }

    const contig = Math.min(this.core.tree.length, this.core.header.hints.contiguousLength)

    if (start + length < contig) {
      const delta = contig - start
      this.missingBlocks.setRange(start, delta, false)
      return
    }

    const rem = start & 32767
    if (rem > 0) {
      start -= rem
      length += rem
    }

    const end = start + Math.min(length, this.core.tree.length)
    while (start < end) {
      const local = bitfield.getBitfield(start)

      if (local && local.bitfield) {
        this.missingBlocks.clear(start, local.bitfield)
      }

      start += 32768
    }
  }

  _resetMissingBlock (index) {
    const bitfield = this.core.skipBitfield === null ? this.core.bitfield : this.core.skipBitfield
    this.missingBlocks.set(index, this._remoteHasBlock(index) && !bitfield.get(index))
  }

  _unclearLocalRange (start, length) {
    if (length === 1) {
      this._resetMissingBlock(start)
      return
    }

    const rem = start & 2097151
    if (rem > 0) {
      start -= rem
      length += rem
    }

    const fixedStart = start

    const end = start + Math.min(length, this.remoteLength)
    while (start < end) {
      const remote = this.remoteBitfield.getBitfield(start)
      if (remote && remote.bitfield) {
        this.missingBlocks.insert(start, remote.bitfield)
      }

      start += 2097152
    }

    this._clearLocalRange(fixedStart, length)
  }

  onrange ({ drop, start, length }) {
    const has = drop === false

    if (drop === true && start < this._remoteContiguousLength) {
      this._remoteContiguousLength = start
    }

    if (start === 0 && drop === false) {
      if (length > this._remoteContiguousLength) this._remoteContiguousLength = length
    } else if (length === 1) {
      const bitfield = this.core.skipBitfield === null ? this.core.bitfield : this.core.skipBitfield
      this.remoteBitfield.set(start, has)
      this.missingBlocks.set(start, has && !bitfield.get(start))
    } else {
      const rangeStart = this.remoteBitfield.findFirst(!has, start)
      const rangeLength = length - (rangeStart - start)

      if (rangeLength > 0) {
        this.remoteBitfield.setRange(rangeStart, rangeLength, has)
        this.missingBlocks.setRange(rangeStart, rangeLength, has)
        if (has) this._clearLocalRange(rangeStart, rangeLength)
      }
    }

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

  _makeRequest (needsUpgrade, priority, minLength) {
    if (needsUpgrade === true && this.replicator._shouldUpgrade(this) === false) {
      return null
    }

    // ensure that the remote has signalled they have the length we request
    if (this.remoteLength < minLength) {
      return null
    }

    if (needsUpgrade === false && this.replicator._autoUpgrade(this) === true) {
      needsUpgrade = true
    }

    return {
      peer: this,
      rt: this.roundtripQueue === null ? 0 : this.roundtripQueue.tick,
      id: 0,
      fork: this.remoteFork,
      block: null,
      hash: null,
      seek: null,
      upgrade: needsUpgrade === false
        ? null
        : { start: this.core.tree.length, length: this.remoteLength - this.core.tree.length },
      // remote manifest check can be removed eventually...
      manifest: this.core.header.manifest === null && this.remoteHasManifest === true,
      priority
    }
  }

  _requestManifest () {
    const req = this._makeRequest(false, 0, 0)
    this._send(req)
  }

  _requestUpgrade (u) {
    const req = this._makeRequest(true, 0, 0)
    if (req === null) return false

    this._send(req)

    return true
  }

  _requestSeek (s) {
    // if replicator is updating the seeks etc, bail and wait for it to drain
    if (this.replicator._updatesPending > 0) return false

    const { length, fork } = this.core.tree

    if (fork !== this.remoteFork) return false

    if (s.seeker.start >= length) {
      const req = this._makeRequest(true, 0, 0)

      // We need an upgrade for the seek, if non can be provided, skip
      if (req === null) return false

      req.seek = this.remoteSupportsSeeks ? { bytes: s.seeker.bytes, padding: s.seeker.padding } : null

      s.inflight.push(req)
      this._send(req)

      return true
    }

    const len = s.seeker.end - s.seeker.start
    const off = s.seeker.start + Math.floor(Math.random() * len)

    for (let i = 0; i < len; i++) {
      let index = off + i
      if (index > s.seeker.end) index -= len

      if (this._remoteHasBlock(index) === false) continue
      if (this.core.bitfield.get(index) === true) continue
      if (!this._hasTreeParent(index)) continue

      // Check if this block is currently inflight - if so pick another
      const b = this.replicator._blocks.get(index)
      if (b !== null && b.inflight.length > 0) continue

      // Block is not inflight, but we only want the hash, check if that is inflight
      const h = this.replicator._hashes.add(index, PRIORITY.NORMAL)
      if (h.inflight.length > 0) continue

      const req = this._makeRequest(false, h.priority, index + 1)
      if (req === null) continue

      const nodes = flatTree.depth(s.seeker.start + s.seeker.end - 1)

      req.hash = { index: 2 * index, nodes }
      req.seek = this.remoteSupportsSeeks ? { bytes: s.seeker.bytes, padding: s.seeker.padding } : null

      s.inflight.push(req)
      h.inflight.push(req)
      this._send(req)

      return true
    }

    this._maybeWant(s.seeker.start, len)
    return false
  }

  _hasTreeParent (index) {
    if (this.remoteLength >= this.core.tree.length) return true

    const ite = flatTree.iterator(index * 2)

    let span = 2
    let length = 0

    while (true) {
      ite.parent()

      const left = (ite.index - ite.factor / 2 + 1) / 2
      length = left + span

      // if larger than local AND larger than remote - they share the root so its ok
      if (length > this.core.tree.length) {
        if (length > this.remoteLength) return true
        break
      }

      // its less than local but larger than remote so skip it
      if (length > this.remoteLength) break

      span *= 2
      const first = this.core.bitfield.findFirst(true, left)
      if (first > -1 && first < length) return true
    }

    // TODO: push to async queue and check against our local merkle tree if we actually can request this block
    return false
  }

  _remoteHasBlock (index) {
    return index < this._remoteContiguousLength || this.remoteBitfield.get(index) === true
  }

  _sendBlockRequest (req, b) {
    req.block = { index: b.index, nodes: 0 }
    this.replicator._markInflight(b.index)

    b.inflight.push(req)
    this.replicator.hotswaps.add(b)
    this._send(req)
  }

  _requestBlock (b) {
    const { length, fork } = this.core.tree

    if (this._remoteHasBlock(b.index) === false || fork !== this.remoteFork) {
      this._maybeWant(b.index)
      return false
    }

    if (!this._hasTreeParent(b.index)) {
      return false
    }

    const req = this._makeRequest(b.index >= length, b.priority, b.index + 1)
    if (req === null) return false

    this._sendBlockRequest(req, b)

    return true
  }

  _requestRangeBlock (index, length) {
    if (this.core.bitfield.get(index) === true || !this._hasTreeParent(index)) return false

    const b = this.replicator._blocks.add(index, PRIORITY.NORMAL)
    if (b.inflight.length > 0) {
      this.missingBlocks.set(index, false) // in case we missed some states just set them ondemand, nbd
      return false
    }

    const req = this._makeRequest(index >= length, b.priority, index + 1)

    // If the request cannot be satisfied, dealloc the block request if no one is subscribed to it
    if (req === null) {
      b.gc()
      return false
    }

    this._sendBlockRequest(req, b)

    // Don't think this will ever happen, as the pending queue is drained before the range queue
    // but doesn't hurt to check this explicitly here also.
    if (b.queued) b.queued = false
    return true
  }

  _findNext (i) {
    if (i < this._remoteContiguousLength) {
      if (this.core.skipBitfield === null) this.replicator._openSkipBitfield()
      i = this.core.skipBitfield.findFirst(false, i)
      if (i < this._remoteContiguousLength && i > -1) return i
      i = this._remoteContiguousLength
    }

    return this.missingBlocks.findFirst(true, i)
  }

  _requestRange (r) {
    const { length, fork } = this.core.tree

    if (r.blocks) {
      let min = -1
      let max = -1

      for (let i = r.start; i < r.end; i++) {
        const index = r.blocks[i]
        if (min === -1 || index < min) min = index
        if (max === -1 || index > max) max = index
        const has = index < this._remoteContiguousLength || this.missingBlocks.get(index) === true
        if (has === true && this._requestRangeBlock(index, length)) return true
      }

      if (min > -1) this._maybeWant(min, max - min)
      return false
    }

    const end = Math.min(this.core.tree.length, Math.min(r.end === -1 ? this.remoteLength : r.end, this.remoteLength))
    if (end <= r.start || fork !== this.remoteFork) return false

    const len = end - r.start
    const off = r.start + (r.linear ? 0 : Math.floor(Math.random() * len))

    let i = off

    while (true) {
      i = this._findNext(i)
      if (i === -1 || i >= end) break

      if (this._requestRangeBlock(i, length)) return true
      i++
    }

    i = r.start

    while (true) {
      i = this._findNext(i)
      if (i === -1 || i >= off) break

      if (this._requestRangeBlock(i, length)) return true
      i++
    }

    this._maybeWant(r.start, len)
    return false
  }

  _requestForkProof (f) {
    const req = this._makeRequest(false, 0, 0)

    req.upgrade = { start: 0, length: this.remoteLength }
    req.manifest = !this.core.header.manifest

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

      if (this._remoteHasBlock(index) === false) continue

      const req = this._makeRequest(false, 0, 0)

      req.hash = { index: 2 * index, nodes: f.batch.want.nodes }

      f.inflight.push(req)
      this._send(req)

      return true
    }

    this._maybeWant(f.batch.want.start, len)
    return false
  }

  _maybeWant (start, length = 1) {
    if (start + length <= this.remoteContiguousLength) return

    let i = Math.floor(start / DEFAULT_SEGMENT_SIZE)
    const n = Math.ceil((start + length) / DEFAULT_SEGMENT_SIZE)

    for (; i < n; i++) {
      if (this.segmentsWanted.has(i)) continue
      this.segmentsWanted.add(i)

      this.wireWant.send({
        start: i * DEFAULT_SEGMENT_SIZE,
        length: DEFAULT_SEGMENT_SIZE
      })
      incrementTx(this.stats.wireWant, this.replicator.stats.wireWant)
    }
  }

  isActive () {
    if (this.paused || this.removed) return false
    return true
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
        if (req.priority === PRIORITY.CANCELLED) return
      }
      if (req.hash !== null && req.fork === fork && req.hash.nodes === 0) {
        req.hash.nodes = await this.core.tree.missingNodes(req.hash.index)
        if (req.priority === PRIORITY.CANCELLED) return

        // nodes === 0, we already have it, bail
        if (req.hash.nodes === 0 && (req.hash.index & 1) === 0) {
          this.inflight--
          this.replicator._resolveHashLocally(this, req)
          return
        }
      }
    } catch (err) {
      this.stream.destroy(err)
      return
    }

    this.wireRequest.send(req)
    incrementTx(this.stats.wireRequest, this.replicator.stats.wireRequest)
  }
}

module.exports = class Replicator {
  static Peer = Peer // hack to be able to access Peer from outside this module

  constructor (core, key, {
    notDownloadingLinger = NOT_DOWNLOADING_SLACK,
    eagerUpgrade = true,
    allowFork = true,
    inflightRange = null,
    onpeerupdate = noop,
    onupload = noop,
    oninvalid = noop
  } = {}) {
    this.key = key
    this.discoveryKey = core.crypto.discoveryKey(key)
    this.core = core
    this.eagerUpgrade = eagerUpgrade
    this.allowFork = allowFork
    this.onpeerupdate = onpeerupdate
    this.onupload = onupload
    this.oninvalid = oninvalid
    this.ondownloading = null // optional external hook for monitoring downloading status
    this.peers = []
    this.findingPeers = 0 // updateable from the outside
    this.destroyed = false
    this.downloading = false
    this.activeSessions = 0

    this.hotswaps = new HotswapQueue()
    this.inflightRange = inflightRange || DEFAULT_MAX_INFLIGHT

    // Note: nodata and unwant not currently tracked
    // tx = transmitted, rx = received
    this.stats = {
      wireSync: { tx: 0, rx: 0 },
      wireRequest: { tx: 0, rx: 0 },
      wireCancel: { tx: 0, rx: 0 },
      wireData: { tx: 0, rx: 0 },
      wireWant: { tx: 0, rx: 0 },
      wireBitfield: { tx: 0, rx: 0 },
      wireRange: { tx: 0, rx: 0 },
      wireExtension: { tx: 0, rx: 0 },
      hotswaps: 0
    }

    this._attached = new Set()
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
    this._manifestPeer = null
    this._hasSession = false
    this._peerSessions = 0
    this._notDownloadingLinger = notDownloadingLinger
    this._downloadingTimer = null

    const self = this
    this._onstreamclose = onstreamclose

    function onstreamclose () {
      self.detachFrom(this.userData)
    }
  }

  updateActivity (inc, session) {
    this.activeSessions += inc
    this.setDownloading(this.activeSessions !== 0, session)
  }

  isDownloading () {
    return this.downloading || !this._inflight.idle
  }

  setDownloading (downloading) {
    clearTimeout(this._downloadingTimer)

    if (this.destroyed) return
    if (downloading || this._notDownloadingLinger === 0) {
      this.setDownloadingNow(downloading)
      return
    }

    this._downloadingTimer = setTimeout(setDownloadingLater, this._notDownloadingLinger, this, downloading)
  }

  setDownloadingNow (downloading) {
    this._downloadingTimer = null
    if (this.downloading === downloading) return
    this.downloading = downloading
    if (!downloading && this.isDownloading()) return

    for (const peer of this.peers) peer.signalUpgrade()

    if (downloading) { // restart channel if needed...
      for (const protomux of this._attached) {
        if (!protomux.stream.handshakeHash) continue
        if (protomux.opened({ protocol: 'hypercore/alpha', id: this.discoveryKey })) continue
        this._makePeer(protomux, true)
      }
    } else {
      for (const peer of this.peers) peer.closeIfIdle()
    }

    if (this.ondownloading !== null && downloading) this.ondownloading()
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
  ontruncate (newLength, truncated) {
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

    for (const peer of this.peers) peer._unclearLocalRange(newLength, truncated)
  }

  // Called externally when a upgrade has been processed
  onupgrade () {
    for (const peer of this.peers) peer.signalUpgrade()
    if (this._blocks.isEmpty() === false) this._resolveBlocksLocally()
    if (this._upgrade !== null) this._resolveUpgradeRequest(null)
    if (this._ranges.length !== 0 || this._seeks.length !== 0) this._updateNonPrimary(true)
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
    const b = this._blocks.add(index, PRIORITY.HIGH)
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

    // Trigger this to see if this is already resolved...
    // Also auto compresses the range based on local bitfield
    clampRange(this.core, r)

    this._ranges.push(r)

    if (r.end !== -1 && r.start >= r.end) {
      this._resolveRangeRequest(r, this._ranges.length - 1)
      return ref
    }

    this.updateAll()

    return ref
  }

  cancel (ref) {
    ref.context.detach(ref, null)
  }

  clearRequests (session, err = null) {
    let cleared = false
    while (session.length > 0) {
      const ref = session[session.length - 1]
      ref.context.detach(ref, err)
      cleared = true
    }

    if (cleared) this.updateAll()
  }

  _addUpgradeMaybe () {
    return this.eagerUpgrade === true ? this._addUpgrade() : this._upgrade
  }

  // TODO: this function is OVER called atm, at each updatePeer/updateAll
  // instead its more efficient to only call it when the conditions in here change - ie on sync/add/remove peer
  // Do this when we have more tests.
  _checkUpgradeIfAvailable () {
    if (this._ifAvailable > 0 && this.peers.length < MAX_PEERS_UPGRADE) return
    if (this._upgrade === null || this._upgrade.refs.length === 0) return
    if (this._hadPeers === false && this.findingPeers > 0) return

    const maxPeers = Math.min(this.peers.length, MAX_PEERS_UPGRADE)

    // check if a peer can upgrade us

    for (let i = 0; i < maxPeers; i++) {
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

    // if something is inflight, wait for that first
    if (this._upgrade.inflight.length > 0) return

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

  _requestDone (id, roundtrip) {
    this._inflight.remove(id, roundtrip)
    if (this.isDownloading() === true) return
    for (const peer of this.peers) peer.signalUpgrade()
  }

  _removePeer (peer) {
    this.peers.splice(this.peers.indexOf(peer), 1)

    if (this._manifestPeer === peer) this._manifestPeer = null

    for (const req of this._inflight) {
      if (req.peer !== peer) continue
      this._inflight.remove(req.id, true)
      this._clearRequest(peer, req)
    }

    if (peer.useSession) this._closeSessionMaybe()

    this.onpeerupdate(false, peer)
    this.updateAll()
  }

  _queueBlock (b) {
    if (b.inflight.length > 0 || b.queued === true) return
    b.queued = true
    this._queued.push(b)
  }

  _resolveHashLocally (peer, req) {
    this._requestDone(req.id, false)
    this._resolveBlockRequest(this._hashes, req.hash.index / 2, null, req)
    this.updatePeer(peer)
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
      removeHotswap(b)
    }
  }

  _resolveBlockRequest (tracker, index, value, req) {
    const b = tracker.remove(index)
    if (b === null) return false

    removeInflight(b.inflight, req)
    removeHotswap(b)
    b.queued = false

    b.resolve(value)

    if (b.inflight.length > 0) { // if anything is still inflight, cancel it
      for (let i = b.inflight.length - 1; i >= 0; i--) {
        const req = b.inflight[i]
        req.peer._cancelRequest(req)
      }
    }

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

    if (removeHotswap(b) === true && b.inflight.length > 0) {
      this.hotswaps.add(b)
    }

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
  async _updateNonPrimary (updateAll) {
    // Check if running, if so skip it and the running one will issue another update for us (debounce)
    while (++this._updatesPending === 1) {
      let len = Math.min(MAX_RANGES, this._ranges.length)

      for (let i = 0; i < len; i++) {
        const r = this._ranges[i]

        clampRange(this.core, r)

        if (r.end !== -1 && r.start >= r.end) {
          this._resolveRangeRequest(r, i--)
          if (len > this._ranges.length) len--
          if (this._ranges.length === MAX_RANGES) updateAll = true
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

      // No additional updates scheduled - break
      if (--this._updatesPending === 0) break
      // Debounce the additional updates - continue
      this._updatesPending = 0
    }

    if (this._inflight.idle || updateAll) this.updateAll()
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
      this._unmarkInflight(req.block.index)
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

  _openSkipBitfield () {
    // technically the skip bitfield gets bits cleared if .clear() is called
    // also which might be in inflight also, but that just results in that section being overcalled shortly
    // worst case, so ok for now

    const bitfield = this.core.openSkipBitfield()

    for (const req of this._inflight) {
      if (req.block) bitfield.set(req.block.index, true) // skip
    }
  }

  _markInflight (index) {
    if (this.core.skipBitfield !== null) this.core.skipBitfield.set(index, true)
    for (const peer of this.peers) peer._markInflight(index)
  }

  _unmarkInflight (index) {
    if (this.core.skipBitfield !== null) this.core.skipBitfield.set(index, this.core.bitfield.get(index))
    for (const peer of this.peers) peer._resetMissingBlock(index)
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

    if (this._manifestPeer === peer && this.core.header.manifest !== null) {
      this._manifestPeer = null
    }

    if (this._seeks.length > 0 || this._ranges.length > 0) this._updateNonPrimary(this._seeks.length > 0)
    this.updatePeer(peer)
  }

  _onwant (peer, start, length) {
    const contig = Math.min(this.core.tree.length, this.core.header.hints.contiguousLength)

    if (start + length < contig || (this.core.tree.length === contig)) {
      peer.wireRange.send({
        drop: false,
        start: 0,
        length: contig
      })
      incrementTx(peer.stats.wireRange, this.stats.wireRange)
      return
    }

    length = Math.min(length, this.core.tree.length - start)

    peer.protomux.cork()

    for (const msg of this.core.bitfield.want(start, length)) {
      peer.wireBitfield.send(msg)
      incrementTx(peer.stats.wireBitfield, this.stats.wireBitfield)
    }

    peer.protomux.uncork()
  }

  async _onreorgdata (peer, req, data) {
    const newBatch = data.upgrade && await this.core.verifyReorg(data)
    const f = this._addReorg(data.fork, peer)

    if (f === null) {
      this.updateAll()
      return
    }

    removeInflight(f.inflight, req)

    if (f.batch) {
      await f.batch.update(data)
    } else if (data.upgrade) {
      f.batch = newBatch

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

  _maybeRequestManifest () {
    return this.core.header.manifest === null && this._manifestPeer === null
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

  _updateHotswap (peer) {
    const maxHotswaps = peer.getMaxHotswapInflight()
    if (!peer.isActive() || peer.inflight >= maxHotswaps) return

    for (const b of this.hotswaps.pick(peer)) {
      if (peer._requestBlock(b) === false) continue
      peer.stats.hotswaps++
      peer.replicator.stats.hotswaps++
      if (peer.inflight >= maxHotswaps) break
    }
  }

  _updatePeer (peer) {
    if (!peer.isActive() || peer.inflight >= peer.getMaxInflight()) {
      return false
    }

    // Eagerly request the manifest even if the remote length is 0. If not 0 we'll get as part of the upgrade request...
    if (this._maybeRequestManifest() === true && peer.remoteLength === 0 && peer.remoteHasManifest === true) {
      this._manifestPeer = peer
      peer._requestManifest()
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
    if (!peer.isActive() || peer.inflight >= peer.getMaxInflight()) {
      return false
    }

    const ranges = new RandomIterator(this._ranges)
    let tried = 0

    for (const r of ranges) {
      if (peer._requestRange(r) === true) {
        return true
      }
      if (++tried >= MAX_RANGES) break
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

    if (this.peers.length > 1 && this._blocks.isEmpty() === false) {
      this._updateHotswap(peer)
    }

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

  _closeSessionMaybe () {
    if (this._hasSession && this._peerSessions === 0) {
      this._hasSession = false
      this.core.active--
    }

    // we were the last active ref, so lets shut things down
    if (this.core.active === 0 && this.core.sessions.length === 0) {
      this.destroy()
      this.core.close().catch(safetyCatch)
      return
    }

    // in case one session is still alive but its been marked for auto close also kill it
    if (this.core.sessions.length === 1 && this.core.active === 1 && this.core.sessions[0].autoClose) {
      this.core.sessions[0].close().catch(safetyCatch)
    }
  }

  attached (protomux) {
    return this._attached.has(protomux)
  }

  ensureSession () {
    if (this._hasSession) return
    this._hasSession = true
    this.core.active++
  }

  attachTo (protomux, useSession) {
    if (this.core.closed) return
    if (useSession) this.ensureSession()

    const makePeer = this._makePeer.bind(this, protomux, useSession)

    this._attached.add(protomux)
    protomux.pair({ protocol: 'hypercore/alpha', id: this.discoveryKey }, makePeer)
    protomux.stream.setMaxListeners(0)
    protomux.stream.on('close', this._onstreamclose)

    if (useSession) this._peerSessions++
    this._ifAvailable++

    protomux.stream.opened.then((opened) => {
      if (useSession) this._peerSessions--
      this._ifAvailable--

      if (opened && !this.destroyed) makePeer()
      else if (useSession) this._closeSessionMaybe()
      this._checkUpgradeIfAvailable()
    })
  }

  detachFrom (protomux) {
    if (this._attached.delete(protomux)) {
      protomux.stream.removeListener('close', this._onstreamclose)
      protomux.unpair({ protocol: 'hypercore/alpha', id: this.discoveryKey })
    }
  }

  destroy () {
    this.destroyed = true

    if (this._downloadingTimer) {
      clearTimeout(this._downloadingTimer)
      this._downloadingTimer = null
    }

    const waiting = []

    while (this.peers.length) {
      const peer = this.peers[this.peers.length - 1]
      this.detachFrom(peer.protomux)
      peer.channel.close() // peer is removed from array in onclose
      waiting.push(peer.channel.fullyClosed())
    }

    for (const protomux of this._attached) {
      this.detachFrom(protomux)
    }

    return Promise.all(waiting)
  }

  _makePeer (protomux, useSession) {
    const replicator = this
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
        { encoding: m.wire.cancel, onmessage: onwirecancel },
        { encoding: m.wire.data, onmessage: onwiredata },
        { encoding: m.wire.noData, onmessage: onwirenodata },
        { encoding: m.wire.want, onmessage: onwirewant },
        { encoding: m.wire.unwant, onmessage: onwireunwant },
        { encoding: m.wire.bitfield, onmessage: onwirebitfield },
        { encoding: m.wire.range, onmessage: onwirerange },
        { encoding: m.wire.extension, onmessage: onwireextension }
      ],
      onopen: onwireopen,
      onclose: onwireclose,
      ondrain: onwiredrain
    })

    if (channel === null) return onnochannel()

    const peer = new Peer(replicator, protomux, channel, useSession, this.inflightRange)
    const stream = protomux.stream

    if (useSession) {
      // session may have been unref'd underneath us
      replicator.ensureSession()
      replicator._peerSessions++
    }

    peer.channel.open({
      seeks: true,
      capability: caps.replicate(stream.isInitiator, this.key, stream.handshakeHash)
    })

    return true

    function onnochannel () {
      if (useSession) replicator._closeSessionMaybe()
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

function removeHotswap (block) {
  if (block.hotswap === null) return false
  block.hotswap.ref.remove(block)
  return true
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

    if (r.end === -1) r.start = start === -1 ? core.tree.length : start
    else if (start === -1 || start >= r.end) r.start = r.end
    else {
      r.start = start

      const end = core.bitfield.lastUnset(r.end - 1)

      if (end === -1 || start >= end + 1) r.end = r.start
      else r.end = end + 1
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

function isCriticalError (err) {
  // TODO: expose .critical or similar on the hypercore errors that are critical (if all not are)
  return err.name === 'HypercoreError'
}

function onwireopen (m, c) {
  return c.userData.onopen(m)
}

function onwireclose (isRemote, c) {
  return c.userData.onclose(isRemote)
}

function onwiredrain (c) {
  return c.userData.ondrain()
}

function onwiresync (m, c) {
  incrementRx(c.userData.stats.wireSync, c.userData.replicator.stats.wireSync)
  return c.userData.onsync(m)
}

function onwirerequest (m, c) {
  incrementRx(c.userData.stats.wireRequest, c.userData.replicator.stats.wireRequest)
  return c.userData.onrequest(m)
}

function onwirecancel (m, c) {
  incrementRx(c.userData.stats.wireCancel, c.userData.replicator.stats.wireCancel)
  return c.userData.oncancel(m)
}

function onwiredata (m, c) {
  incrementRx(c.userData.stats.wireData, c.userData.replicator.stats.wireData)
  return c.userData.ondata(m)
}

function onwirenodata (m, c) {
  return c.userData.onnodata(m)
}

function onwirewant (m, c) {
  incrementRx(c.userData.stats.wireWant, c.userData.replicator.stats.wireWant)
  return c.userData.onwant(m)
}

function onwireunwant (m, c) {
  return c.userData.onunwant(m)
}

function onwirebitfield (m, c) {
  incrementRx(c.userData.stats.wireBitfield, c.userData.replicator.stats.wireBitfield)
  return c.userData.onbitfield(m)
}

function onwirerange (m, c) {
  incrementRx(c.userData.stats.wireRange, c.userData.replicator.stats.wireRange)
  return c.userData.onrange(m)
}

function onwireextension (m, c) {
  incrementRx(c.userData.stats.wireExtension, c.userData.replicator.stats.wireExtension)
  return c.userData.onextension(m)
}

function setDownloadingLater (repl, downloading, session) {
  repl.setDownloadingNow(downloading, session)
}

function isBlockRequest (req) {
  return req !== null && req.block !== null
}

function isUpgradeRequest (req) {
  return req !== null && req.upgrade !== null
}

function incrementTx (stats1, stats2) {
  stats1.tx++
  stats2.tx++
}

function incrementRx (stats1, stats2) {
  stats1.rx++
  stats2.rx++
}
