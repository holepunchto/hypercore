const { EventEmitter } = require('events')
const Peer = require('./peer')
const RemoteBitfield = require('./remote-bitfield')
const RandomIterator = require('random-array-iterator')

const PKG = require('../package.json')
const USER_AGENT = PKG.name + '/' + PKG.version + '@nodejs'

class RemoteState {
  constructor (core) {
    this.handshake = null
    this.receivedInfo = false
    this.inflight = 0
    this.maxInflight = 16
    this.bitfield = new RemoteBitfield()
    this.length = 0
    this.fork = 0
    this.remoteExtensions = core.extensions.remote()
  }
}

class InvertedPromise {
  constructor (resolve, reject, index) {
    this.index = index
    this.resolve = resolve
    this.reject = reject
  }
}

class Request {
  constructor (index, seek) {
    this.peer = null
    this.index = index
    this.seek = seek
    this.value = seek === 0
    this.promises = []
  }

  createPromise () {
    return new Promise((resolve, reject) => {
      this.promises.push(new InvertedPromise(resolve, reject, this.promises.length))
    })
  }

  resolve (val) {
    for (let i = 0; i < this.promises.length; i++) {
      this.promises[i].resolve(val)
    }
  }

  reject (err) {
    for (let i = 0; i < this.promises.length; i++) {
      this.promises[i].reject(err)
    }
  }
}

class Upgrade {
  constructor (minLength) {
    this.minLength = minLength
    this.promises = []
  }

  update (peers, fork) {
    for (const peer of peers) {
      if (peer.state.length >= this.minLength && !paused(peer, fork)) return true
      if (!peer.state.receivedInfo) return true
    }

    return false
  }

  createPromise () {
    return new Promise((resolve, reject) => {
      this.promises.push(new InvertedPromise(resolve, reject, this.promises.length))
    })
  }

  resolve (val) {
    for (let i = 0; i < this.promises.length; i++) {
      this.promises[i].resolve(val)
    }
  }

  reject (err) {
    for (let i = 0; i < this.promises.length; i++) {
      this.promises[i].reject(err)
    }
  }
}

class UpgradeLock {
  constructor (peer, length) {
    this.peer = peer
    this.length = length
    this.resolve = null
    this.promise = new Promise((resolve) => { this.resolve = resolve })
  }
}

class Seek {
  constructor (seeker) {
    this.request = null
    this.seeker = seeker
    this.promise = null
  }

  async update () {
    const res = await this.seeker.update()
    if (!res) return false
    this.promise.resolve(res)
    return true
  }

  createPromise () {
    return new Promise((resolve, reject) => {
      this.promise = new InvertedPromise(resolve, reject, 0)
    })
  }
}

class Range {
  constructor (start, end, linear, ranges) {
    this.start = start
    this.end = end
    this.linear = linear
    this.promise = null

    this._inv = null
    this._start = start // can be updated
    this._index = ranges.length
    this._ranges = ranges
    this._resolved = false
    this._error = null
  }

  contains (req) {
    return this._start <= req.index && req.index < this.end
  }

  update (bitfield) {
    if (this.end === -1) {
      while (bitfield.get(this._start)) this._start++
      return false
    }

    for (; this._start < this.end; this._start++) {
      if (!bitfield.get(this._start)) return false
    }

    return true
  }

  resolve (done) {
    this._done(null, done)
  }

  destroy (err) {
    this._done(err, false)
  }

  downloaded () {
    if (this.promise) return this.promise
    if (this._ranges) return this._makePromise()
    if (this._error !== null) return Promise.reject(this._error)
    return Promise.resolve(this._resolved)
  }

  _done (err, done) {
    if (!this._ranges) return

    this._ranges[this._index] = this._ranges[this._ranges.length - 1]
    this._ranges.pop()
    this._ranges = null
    this._resolved = done
    this._error = err

    if (this._inv === null) return
    if (err) this._inv.reject(err)
    else this._inv.resolve(done)
  }

  _makePromise () {
    this.promise = new Promise((resolve, reject) => {
      this._inv = new InvertedPromise(resolve, reject, 0)
    })

    return this.promise
  }
}

class RequestPool {
  constructor (core) {
    this.core = core
    this.pending = []
    this.seeks = []
    this.ranges = []
    this.requests = new Map()
    this.upgrading = null
    this.unforking = null
    this.eagerUpgrades = true
  }

  // We could make this faster by using some memory of each peer to store what they are involved in,
  // but that might not be worth the cost/complexity.
  clear (peer) {
    for (const seek of this.seeks) {
      if (seek.request && seek.request.peer === peer) {
        // TODO: should prob remove cancel this request all together if no one else wants it
        // and nothing is inflight
        seek.request = null
      }
    }
    for (const req of this.requests.values()) {
      if (req.peer === peer) {
        req.peer = null
        this.pending.push(req)
      }
    }
    if (this.upgrading && this.upgrading.peer === peer) {
      this.upgrading.resolve()
      this.upgrading = null
    }
  }

  update (peer) {
    if (peer.state.inflight >= peer.state.maxInflight) return false

    // technically we'd like to run seeks at the same custom prio as reqs
    // but this is a lot simpler and they should run asap anyway as they
    // are super low cost (hash only request)
    for (const seek of this.seeks) {
      if (this.updateSeek(peer, seek)) return true
    }

    if (this.pendingUpgrade) {
      if (this.updateUpgrade(peer)) return true
    }

    const pending = new RandomIterator(this.pending) // can be cached
    for (const req of pending) {
      if (this.updatePeer(peer, req)) {
        pending.dequeue()
        return true
      }
    }

    const ranges = new RandomIterator(this.ranges) // can be cached
    for (const range of ranges) {
      if (this.updateRange(peer, range)) return true
    }

    if (this.eagerUpgrades && !this.upgrading) {
      return this.updateUpgrade(peer)
    }

    return false
  }

  updateSeek (peer, seek) {
    if (seek.request) return false
    seek.request = this._requestRange(peer, seek.seeker.start, seek.seeker.end, seek.seeker.bytes)
    return seek.request !== null
  }

  updatePeer (peer, req) {
    const remote = peer.state.bitfield
    const local = this.core.bitfield

    if (!remote.get(req.index) || local.get(req.index)) return false

    this.send(peer, req)
    return true
  }

  updateRange (peer, range) {
    const end = range.end === -1 ? peer.state.length : range.end
    if (end <= range._start) return false

    if (range.linear) return !!this._requestRange(peer, range._start, end, 0)

    const r = range._start + Math.floor(Math.random() * (end - range._start))
    return !!(this._requestRange(peer, r, end, 0) || this._requestRange(peer, range._start, r, 0))
  }

  updateUpgrade (peer) {
    const minLength = this.pendingUpgrade
      ? this.pendingUpgrade.minLength
      : this.core.length + 1

    if (this.upgrading || peer.state.length < minLength) return false

    this.upgrading = new UpgradeLock(peer, peer.state.length)

    const data = {
      seek: null,
      block: null,
      upgrade: { start: this.core.length, length: peer.state.length - this.core.length }
    }

    peer.request(data)
    return true
  }

  checkTimeouts (peers) {
    if (!this.pendingUpgrade || this.upgrading) return
    if (this.pendingUpgrade.update(peers, this.core.fork)) return
    this.pendingUpgrade.resolve(false)
    this.pendingUpgrade = null
  }

  _requestRange (peer, start, end, seek) {
    const remote = peer.state.bitfield
    const local = this.core.bitfield

    // TODO: use 0 instead of -1 as end=0 should never be added!
    if (end === -1) end = peer.state.length

    for (let i = start; i < end; i++) {
      if (!remote.get(i) || local.get(i)) continue
      // TODO: if this was a NO_VALUE request, retry if no blocks can be found elsewhere
      if (this.requests.has(i)) continue

      // TODO: if seeking and i >= core.length, let that takes precendance in the upgrade req
      const req = new Request(i, i < this.core.length ? seek : 0)
      this.requests.set(i, req)
      this.send(peer, req)
      return req
    }

    return null
  }

  // send handles it's own errors so we do not need to await/catch it
  async send (peer, req) {
    req.peer = peer
    peer.state.inflight++ // TODO: a non value request should count less than a value one

    // TODO: also check if remote can even upgrade us lol
    let needsUpgrade = peer.state.length > this.core.length || !!(!this.upgrading && this.pendingUpgrade)
    const fork = this.core.fork

    while (needsUpgrade) {
      if (!this.upgrading) {
        // TODO: if the peer fails, we need to resolve the promise as well woop woop
        // so we need some tracking mechanics for upgrades in general.
        this.upgrading = new UpgradeLock(peer, peer.state.length)
        break
      }
      if (req.index < this.core.length) {
        needsUpgrade = false
        break
      }
      await this.upgrading.promise
      needsUpgrade = peer.state.length > this.core.length || !!(!this.upgrading && this.pendingUpgrade)
    }

    const data = {
      seek: req.seek ? { bytes: req.seek } : null,
      block: { index: req.index, nodes: 0, value: req.value },
      upgrade: needsUpgrade ? { start: this.core.length, length: peer.state.length - this.core.length } : null
    }

    if (data.block.index < this.core.length) {
      try {
        data.block.nodes = await this.core.tree.nodes(data.block.index * 2)
      } catch (err) {
        console.error('TODO handle me:', err.stack)
      }
    }

    if (fork !== this.core.fork || paused(peer, this.core.fork)) {
      this.requests.delete(req.index)
      return
    }

    peer.request(data)
  }

  async _onupgrade (proof, peer) {
    if (!this.upgrading || !proof.upgrade) return
    if (this.unforking) return

    await this.core.verify(proof, peer)

    // TODO: validate that we actually upgraded our length as well
    this.upgrading.resolve()
    this.upgrading = null

    if (this.pendingUpgrade) {
      this.pendingUpgrade.resolve(true)
      this.pendingUpgrade = null
    }

    if (this.seeks.length) await this._updateSeeks(null)

    this.update(peer)
  }

  async _onfork (proof, peer) {
    // TODO: if proof is from a newer fork than currently unforking, restart

    if (this.unforking) {
      await this.unforking.update(proof)
    } else {
      const reorg = await this.core.tree.reorg(proof)
      const verified = this.core.verifySignature(reorg.signable(), reorg.signature)
      if (!verified) throw new Error('Remote signature could not be verified')
      this.unforking = reorg
    }

    if (!this.unforking.finished) {
      for (let i = this.unforking.want.start; i < this.unforking.want.end; i++) {
        if (peer.state.bitfield.get(i)) {
          const data = {
            fork: this.unforking.fork,
            seek: null,
            block: { index: i, nodes: this.unforking.want.nodes },
            upgrade: null
          }
          peer.request(data)
          return
        }
      }
      return
    }

    await this.core.verifyFork(this.unforking)
    this.unforking = null

    // reset ranges, also need to reset seeks etc
    for (const r of this.ranges) {
      r._start = 0
    }
    // TODO: we gotta clear out old requests as well here pointing at the old fork
  }

  async ondata (proof, peer) {
    // technically if the remote peer pushes a DATA someone else requested inflight can go to zero
    if (peer.state.inflight > 0) peer.state.inflight--

    // if we get a message from another fork, maybe "unfork".
    if (peer.state.fork !== this.core.fork) {
      // TODO: user should opt-in to this behaivour
      if (peer.state.fork > this.core.fork) return this._onfork(proof, peer)
      return
    }

    // ignore incoming messages during an unfork.
    if (this.unforking) return

    if (!proof.block) return this._onupgrade(proof, peer)

    const { index, value } = proof.block
    const req = this.requests.get(index)

    // no push allowed, TODO: add flag to allow pushes
    if (!req || req.peer !== peer || (value && !req.value) || (proof.upgrade && !this.upgrading)) return

    try {
      await this.core.verify(proof, peer)
    } catch (err) {
      this.requests.delete(index)
      throw err
    }

    // TODO: validate that we actually upgraded our length as well
    if (proof.upgrade) {
      this.upgrading.resolve()
      this.upgrading = null

      if (this.pendingUpgrade) {
        this.pendingUpgrade.resolve(true)
        this.pendingUpgrade = null
      }
    }

    // if our request types match, clear inflight, otherwise we upgraded a hash req to a value req
    const resolved = req.value === !!value
    if (resolved) {
      this.requests.delete(index)
      req.resolve(value)
    }

    if (this.seeks.length) await this._updateSeeks(req)

    // TODO: only do this for active ranges, ie ranges with inflight reqs...
    for (let i = 0; i < this.ranges.length; i++) {
      const r = this.ranges[i]
      if (!r.contains(req)) continue
      if (!r.update(this.core.bitfield)) continue
      r.resolve(true)
      i--
    }

    this.update(peer)
  }

  async _updateSeeks (req) {
    for (let i = 0; i < this.seeks.length; i++) {
      const seek = this.seeks[i]

      // I think there is a race condition here so fix that in the seeker, so it's always in a consistent state
      // To repro just remove the pop, and it sends weird reqs
      if (await seek.update()) {
        if (this.seeks.length > 1 && i < this.seeks.length - 1) {
          this.seeks[i] = this.seeks[this.seeks.length - 1]
          i--
        }
        this.seeks.pop()
      }
      if (req !== null && seek.request === req) seek.request = null
    }
  }

  upgrade () {
    if (this.pendingUpgrade) return this.pendingUpgrade.createPromise()
    this.pendingUpgrade = new Upgrade(this.core.length + 1)
    return this.pendingUpgrade.createPromise()
  }

  range (start, end, linear) {
    const range = new Range(start, end, linear, this.ranges)
    this.ranges.push(range)
    if (range.update(this.core.bitfield)) range.resolve(true)
    return range
  }

  seek (seeker) {
    const s = new Seek(seeker)
    this.seeks.push(s)
    return s.createPromise()
  }

  block (index) {
    const e = this.requests.get(index)

    if (e) {
      if (!e.value) {
        e.value = true
        if (e.peer) this.send(e.peer, e)
      }

      return e.createPromise()
    }

    const r = new Request(index, 0)

    this.requests.set(index, r)
    this.pending.push(r)

    return r.createPromise()
  }
}

module.exports = class Replicator extends EventEmitter {
  constructor (core) {
    super()
    this.core = core
    this.peers = []
    this.requests = new RequestPool(core)
    this.updating = null
  }

  createStream () {
    const peer = new Peer(this, new RemoteState(this.core))

    this.peers.push(peer)
    peer.stream.on('close', () => {
      this.peers.splice(this.peers.indexOf(peer), 1)
      this.requests.clear(peer)
      this.emit('peer-remove', peer)
      this.updateAll()
    })

    this.core.opening.then(() => {
      peer.handshake({ protocolVersion: 0, userAgent: USER_AGENT })
      peer.options({ extensions: this.core.extensions.names() })
      peer.info({ length: this.core.length, fork: this.core.fork })

      // YOLO send over all the pages for now
      const p = pages(this.core)
      for (let index = 0; index < p.length; index++) {
        peer.have({ index, bitfield: p[index] })
      }
    })

    return peer.stream
  }

  broadcastBlock (block) {
    const msg = { block }
    for (const peer of this.peers) peer.have(msg)
  }

  broadcastInfo () {
    const msg = { length: this.core.length, fork: this.core.fork }
    for (const peer of this.peers) peer.info(msg)
    this.updateAll()
  }

  broadcastOptions () {
    const msg = { extensions: this.core.extensions.names() }
    for (const peer of this.peers) peer.options(msg)
  }

  requestUpgrade () {
    const promise = this.requests.upgrade()
    this.updateAll()
    return promise
  }

  requestSeek (seeker) {
    if (typeof seeker === 'number') seeker = this.core.tree.seek(seeker)
    const promise = this.requests.seek(seeker)
    this.updateAll()
    return promise
  }

  requestBlock (index) {
    const promise = this.requests.block(index)
    this.updateAll()
    return promise
  }

  requestRange (start, end) {
    const range = this.requests.range(start, end)
    this.updateAll()
    return range
  }

  updateAll () {
    const peers = new RandomIterator(this.peers)
    for (const peer of peers) {
      if (paused(peer, this.core.fork)) continue
      if (this.requests.update(peer)) peers.requeue()
    }

    // TODO: this is a silly way of doing it
    this.requests.checkTimeouts(this.peers)
  }

  onhandshake (handshake, peer) {
    peer.state.handshake = handshake
    this.emit('peer-add', peer)
  }

  oninfo ({ length, fork }, peer) {
    const len = peer.state.length
    const forked = peer.state.fork !== fork

    peer.state.length = length
    peer.state.fork = fork

    if (forked) {
      for (let i = peer.state.length; i < len; i++) {
        peer.state.bitfield.set(i, false)
      }

      if (fork > this.core.fork) peer.request({ fork, upgrade: { start: 0, length } })
    }

    if (!peer.state.receivedInfo) {
      peer.state.receivedInfo = true
    }

    // TODO: do we need to update ALL peers here? prob not
    this.updateAll()
  }

  onhave ({ block, index, bitfield }, peer) {
    if (bitfield) {
      if (bitfield.length < 4096) bitfield = Buffer.concat([bitfield, Buffer.alloc(4096 - bitfield.length)])
      peer.state.bitfield.pages.set(index, new Uint32Array(bitfield.buffer, bitfield.byteOffset, 1024))
    } else {
      peer.state.bitfield.set(block, true)
    }

    // TODO: do we need to update ALL peers here? prob not
    this.updateAll()
  }

  async ondata (proof, peer) {
    try {
      await this.requests.ondata(proof, peer)
    } catch (err) {
      // TODO: the request pool should have this cap, so we can just bubble it up
      this.updateAll()
      throw err
    }
  }

  async onrequest (req, peer) {
    const fork = req.fork || peer.state.fork
    if (fork !== this.core.fork) return

    const proof = await this.core.proof(req)
    peer.data(proof)
  }

  onoptions ({ extensions }, peer) {
    peer.state.remoteExtensions.update(extensions)
  }

  onextension ({ id, data }, peer) {
    peer.state.remoteExtensions.onmessage(id, data, peer)
  }

  onerror (err, peer) {
    peer.stream.destroy(err)
  }
}

function paused (peer, fork) {
  return peer.state.fork !== fork
}

function pages (core) {
  const all = core.bitfield.bitfield
  const len = Math.ceil(core.length / 32)
  const res = []

  for (let i = 0; i < len; i += 1024) {
    const p = all.subarray(i, Math.min(len, i + 1024))
    res.push(Buffer.from(p.buffer, p.byteOffset, p.byteLength))
  }

  return res
}
