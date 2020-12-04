const Peer = require('./peer')
const RemoteBitfield = require('./remote-bitfield')
const RandomIterator = require('random-array-iterator')

const PKG = require('../package.json')
const USER_AGENT = PKG.name + '/' + PKG.version + '@nodejs'

class RemoteState {
  constructor () {
    this.handshake = null
    this.inflight = 0
    this.maxInflight = 16
    this.bitfield = new RemoteBitfield()
    this.length = 0
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

  resolve () {
    if (!this._ranges) return

    this._ranges[this._index] = this._ranges[this._ranges.length - 1]
    this._ranges.pop()
    this._ranges = null

    if (this._inv !== null) this._inv.resolve()
  }

  downloaded () {
    if (this.promise) return this.promise
    if (!this._ranges) return Promise.resolve()
    return this._makePromise()
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
    this.upgradingResolve = null
  }

  update (peer) {
    if (peer.state.inflight >= peer.state.maxInflight) return false

    // technically we'd like to run seeks at the same custom prio as reqs
    // but this is a lot simpler and they should run asap anyway as they
    // are super low cost (hash only request)
    for (const seek of this.seeks) {
      if (this.updateSeek(peer, seek)) return true
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
    let needsUpgrade = peer.state.length > this.core.length

    while (needsUpgrade) {
      if (!this.upgrading) {
        // TODO: if the peer fails, we need to resolve the promise as well woop woop
        // so we need some tracking mechanics for upgrades in general.
        this.upgrading = new Promise((resolve) => { this.upgradingResolve = resolve })
        break
      }
      await this.upgrading
      needsUpgrade = peer.state.length > this.core.length
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

    peer.request(data)
  }

  async ondata (proof, peer) {
    if (proof.block === null) {
      // TODO: impl UPGRADE_ONLY data messages!
      return
    }

    // technically if the remote peer pushes a DATA someone else requested inflight can go to zero
    if (peer.state.inflight > 0) peer.state.inflight--

    const { index, value } = proof.block
    const req = this.requests.get(index)

    // no push allowed, TODO: add flag to allow pushes
    if (!req || req.peer !== peer || (value && !req.value)) return

    try {
      await this.core.verify(proof, peer)
    } catch (err) {
      this.requests.delete(index)
      throw err
    }

    if (proof.upgrade) {
      this.upgradingResolve()
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
      r.resolve()
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
      if (seek.request === req) seek.request = null
    }
  }

  range (start, end, linear) {
    const range = new Range(start, end, linear, this.ranges)
    this.ranges.push(range)
    if (range.update(this.core.bitfield)) range.resolve()
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

module.exports = class Replicator {
  constructor (core) {
    this.core = core
    this.peers = []
    this.requests = new RequestPool(core)
    this.updating = null
  }

  createStream () {
    const peer = new Peer(this, new RemoteState())

    this.peers.push(peer)
    peer.stream.on('close', () => this.peers.splice(this.peers.indexOf(peer), 1))

    this.core.opening.then(() => {
      peer.handshake({ protocolVersion: 0, userAgent: USER_AGENT })
      if (this.core.length > 0) peer.info({ length: this.core.length })

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

  broadcastLength (length) {
    const msg = { length }
    for (const peer of this.peers) peer.info(msg)
    this.updateAll()
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
      if (this.requests.update(peer)) peers.requeue()
    }
  }

  onhandshake (handshake, peer) {
    peer.state.handshake = handshake
  }

  oninfo ({ length }, peer) {
    peer.state.length = length

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
    const proof = await this.core.proof(req)
    peer.data(proof)
  }

  onerror (err, peer) {
    peer.stream.destroy(err)
  }
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
