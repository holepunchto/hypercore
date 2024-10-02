const hypercoreCrypto = require('hypercore-crypto')
const b4a = require('b4a')
const unslab = require('unslab')
const Oplog = require('./oplog')
const BigHeader = require('./big-header')
const Mutex = require('./mutex')
const MerkleTree = require('./merkle-tree')
const BlockStore = require('./block-store')
const Bitfield = require('./bitfield')
const RemoteBitfield = require('./remote-bitfield')
const Info = require('./info')
const { BAD_ARGUMENT, STORAGE_EMPTY, STORAGE_CONFLICT, INVALID_OPERATION, INVALID_SIGNATURE, INVALID_CHECKSUM } = require('hypercore-errors')
const m = require('./messages')
const Verifier = require('./verifier')
const audit = require('./audit')

module.exports = class Core {
  constructor (header, compat, crypto, oplog, bigHeader, tree, blocks, bitfield, verifier, sessions, legacy, globalCache, onupdate, onconflict) {
    this.onupdate = onupdate
    this.onconflict = onconflict
    this.preupdate = null
    this.header = header
    this.compat = compat
    this.crypto = crypto
    this.oplog = oplog
    this.bigHeader = bigHeader
    this.tree = tree
    this.blocks = blocks
    this.bitfield = bitfield
    this.verifier = verifier
    this.truncating = 0
    this.updating = false
    this.closed = false
    this.skipBitfield = null
    this.active = sessions.length
    this.sessions = sessions
    this.globalCache = globalCache

    this._manifestFlushed = !!header.manifest
    this._maxOplogSize = 65536
    this._autoFlush = 1
    this._verifies = null
    this._verifiesFlushed = null
    this._mutex = new Mutex()
    this._legacy = legacy
  }

  static async open (storage, opts = {}) {
    const oplogFile = storage('oplog')
    const treeFile = storage('tree')
    const bitfieldFile = storage('bitfield')
    const dataFile = storage('data')
    const headerFile = storage('header')

    try {
      return await this.resume(oplogFile, treeFile, bitfieldFile, dataFile, headerFile, opts)
    } catch (err) {
      await closeAll(oplogFile, treeFile, bitfieldFile, dataFile, headerFile)
      throw err
    }
  }

  static async resume (oplogFile, treeFile, bitfieldFile, dataFile, headerFile, opts) {
    let overwrite = opts.overwrite === true

    const force = opts.force === true
    const createIfMissing = opts.createIfMissing !== false
    const crypto = opts.crypto || hypercoreCrypto
    // kill this flag soon
    const legacy = !!opts.legacy

    const oplog = new Oplog(oplogFile, {
      headerEncoding: m.oplog.header,
      entryEncoding: m.oplog.entry,
      readonly: opts.readonly
    })

    // default to true for now if no manifest is provided
    let compat = opts.compat === true || (opts.compat !== false && !opts.manifest)

    let { header, entries } = await oplog.open()

    if (force && opts.key && header && !b4a.equals(header.key, opts.key)) {
      overwrite = true
    }

    const bigHeader = new BigHeader(headerFile)

    if (!header || overwrite) {
      if (!createIfMissing) {
        throw STORAGE_EMPTY('No Hypercore is stored here')
      }

      if (compat) {
        if (opts.key && opts.keyPair && !b4a.equals(opts.key, opts.keyPair.publicKey)) {
          throw BAD_ARGUMENT('Key must match publicKey when in compat mode')
        }
      }

      const keyPair = opts.keyPair || (opts.key ? null : crypto.keyPair())
      const defaultManifest = !opts.manifest && (!!opts.compat || !opts.key || !!(keyPair && b4a.equals(opts.key, keyPair.publicKey)))
      const manifest = defaultManifest ? Verifier.defaultSignerManifest(opts.key || keyPair.publicKey) : Verifier.createManifest(opts.manifest)

      header = {
        external: null,
        key: opts.key || (compat ? manifest.signers[0].publicKey : Verifier.manifestHash(manifest)),
        manifest,
        keyPair: keyPair ? { publicKey: keyPair.publicKey, secretKey: keyPair.secretKey || null } : null,
        userData: [],
        tree: {
          fork: 0,
          length: 0,
          rootHash: null,
          signature: null
        },
        hints: {
          reorgs: [],
          contiguousLength: 0
        }
      }

      await flushHeader(oplog, bigHeader, header)
    } else if (header.external) {
      header = await bigHeader.load(header.external)
    }

    // unslab the long lived buffers to avoid keeping the slab alive
    header.key = unslab(header.key)
    header.tree.rootHash = unslab(header.tree.rootHash)
    header.tree.signature = unslab(header.tree.signature)

    if (header.keyPair) {
      header.keyPair.publicKey = unslab(header.keyPair.publicKey)
      header.keyPair.secretKey = unslab(header.keyPair.secretKey)
    }

    if (opts.manifest) {
      // if we provide a manifest and no key, verify that the stored key is the same
      if (!opts.key && !Verifier.isValidManifest(header.key, Verifier.createManifest(opts.manifest))) {
        throw STORAGE_CONFLICT('Manifest does not hash to provided key')
      }

      if (!header.manifest) header.manifest = opts.manifest
    }

    if (opts.key && !b4a.equals(header.key, opts.key)) {
      throw STORAGE_CONFLICT('Another Hypercore is stored here')
    }

    // if we signalled compat, but already now this core isn't disable it
    if (compat && header.manifest && !Verifier.isCompat(header.key, header.manifest)) {
      compat = false
    } else if (!compat && header.manifest && Verifier.isCompat(header.key, header.manifest)) {
      compat = true
    }

    const prologue = header.manifest ? header.manifest.prologue : null

    const tree = await MerkleTree.open(treeFile, { crypto, prologue, ...header.tree })
    const bitfield = await Bitfield.open(bitfieldFile)
    const blocks = new BlockStore(dataFile, tree)

    if (overwrite) {
      await tree.clear()
      await blocks.clear()
      await bitfield.clear()
      entries = []
    }

    // compat from earlier version that do not store contig length
    if (header.hints.contiguousLength === 0) {
      while (bitfield.get(header.hints.contiguousLength)) header.hints.contiguousLength++
    }

    // to unslab
    if (header.manifest) header.manifest = Verifier.createManifest(header.manifest)

    const verifier = header.manifest ? new Verifier(header.key, header.manifest, { crypto, legacy }) : null

    for (const e of entries) {
      if (e.userData) {
        updateUserData(header.userData, e.userData.key, e.userData.value)
      }

      if (e.treeNodes) {
        for (const node of e.treeNodes) {
          tree.addNode(node)
        }
      }

      if (e.bitfield) {
        bitfield.setRange(e.bitfield.start, e.bitfield.length, !e.bitfield.drop)
        updateContig(header, e.bitfield, bitfield)
      }

      if (e.treeUpgrade) {
        const batch = await tree.truncate(e.treeUpgrade.length, e.treeUpgrade.fork)
        batch.ancestors = e.treeUpgrade.ancestors
        batch.signature = unslab(e.treeUpgrade.signature)
        addReorgHint(header.hints.reorgs, tree, batch)
        batch.commit()

        header.tree.length = tree.length
        header.tree.fork = tree.fork
        header.tree.rootHash = tree.hash()
        header.tree.signature = tree.signature
      }
    }

    for (const entry of header.userData) {
      entry.value = unslab(entry.value)
    }

    return new this(header, compat, crypto, oplog, bigHeader, tree, blocks, bitfield, verifier, opts.sessions || [], legacy, opts.globalCache || null, opts.onupdate || noop, opts.onconflict || noop)
  }

  async audit () {
    await this._mutex.lock()

    try {
      await this._flushOplog()
      const corrections = await audit(this)
      if (corrections.blocks || corrections.tree) await this._flushOplog()
      return corrections
    } finally {
      await this._mutex.unlock()
    }
  }

  async setManifest (manifest) {
    await this._mutex.lock()

    try {
      if (manifest && this.header.manifest === null) {
        if (!Verifier.isValidManifest(this.header.key, manifest)) throw INVALID_CHECKSUM('Manifest hash does not match')
        this._setManifest(Verifier.createManifest(manifest), null)
        await this._flushOplog()
      }
    } finally {
      this._mutex.unlock()
    }
  }

  _setManifest (manifest, keyPair) {
    if (!manifest && b4a.equals(keyPair.publicKey, this.header.key)) manifest = Verifier.defaultSignerManifest(this.header.key)
    if (!manifest) return

    const verifier = new Verifier(this.header.key, manifest, { crypto: this.crypto, legacy: this._legacy })

    if (verifier.prologue) this.tree.setPrologue(verifier.prologue)

    this.header.manifest = manifest
    this.compat = verifier.compat
    this.verifier = verifier
    this._manifestFlushed = false

    this.onupdate(0b10000, null, null, null)
  }

  _shouldFlush () {
    // TODO: make something more fancy for auto flush mode (like fibonacci etc)
    if (--this._autoFlush <= 0 || this.oplog.byteLength >= this._maxOplogSize) {
      this._autoFlush = 4
      return true
    }

    if (!this._manifestFlushed && this.header.manifest) {
      this._manifestFlushed = true
      return true
    }

    return false
  }

  async copyPrologue (src, { additional = [] } = {}) {
    await this._mutex.lock()

    try {
      await src._mutex.lock()
    } catch (err) {
      this._mutex.unlock()
      throw err
    }

    try {
      const prologue = this.header.manifest && this.header.manifest.prologue
      if (!prologue) throw INVALID_OPERATION('No prologue present')

      const srcLength = prologue.length - additional.length
      const srcBatch = srcLength !== src.tree.length ? await src.tree.truncate(srcLength) : src.tree.batch()
      const srcRoots = srcBatch.roots.slice(0)
      const srcByteLength = srcBatch.byteLength

      for (const blk of additional) srcBatch.append(blk)

      if (!b4a.equals(srcBatch.hash(), prologue.hash)) throw INVALID_OPERATION('Source tree is conflicting')

      // all hashes are correct, lets copy

      const entry = {
        userData: null,
        treeNodes: srcRoots,
        treeUpgrade: null,
        bitfield: null
      }

      if (additional.length) {
        await this.blocks.putBatch(srcLength, additional, srcByteLength)
        entry.treeNodes = entry.treeNodes.concat(srcBatch.nodes)
        entry.bitfield = {
          drop: false,
          start: srcLength,
          length: additional.length
        }
      }

      await this.oplog.append([entry], false)
      this.tree.addNodes(entry.treeNodes)

      if (this.header.tree.length < srcBatch.length) {
        this.header.tree.length = srcBatch.length
        this.header.tree.rootHash = srcBatch.hash()

        this.tree.length = srcBatch.length
        this.tree.byteLength = srcBatch.byteLength
        this.tree.roots = srcBatch.roots
        this.onupdate(0b0001, null, null, null)
      }

      if (entry.bitfield) {
        this._setBitfieldRange(entry.bitfield.start, entry.bitfield.length, true)
        this.onupdate(0, entry.bitfield, null, null)
      }

      await this._flushOplog()

      // no more additional blocks now and we should be consistant on disk
      // copy over all existing segments...

      let segmentEnd = 0

      while (segmentEnd < srcLength) {
        const segmentStart = maximumSegmentStart(segmentEnd, src.bitfield, this.bitfield)
        if (segmentStart >= srcLength || segmentStart < 0) break

        // max segment is 65536 to avoid running out of memory
        segmentEnd = Math.min(segmentStart + 65536, srcLength, minimumSegmentEnd(segmentStart, src.bitfield, this.bitfield))

        const treeNodes = await src.tree.getNeededNodes(srcLength, segmentStart, segmentEnd)
        const bitfield = {
          drop: false,
          start: segmentStart,
          length: segmentEnd - segmentStart
        }

        const segment = []
        for (let i = segmentStart; i < segmentEnd; i++) {
          const blk = await src.blocks.get(i)
          segment.push(blk)
        }

        const offset = await src.tree.byteOffset(2 * segmentStart)
        await this.blocks.putBatch(segmentStart, segment, offset)

        const entry = {
          userData: null,
          treeNodes,
          treeUpgrade: null,
          bitfield
        }

        await this.oplog.append([entry], false)
        this.tree.addNodes(treeNodes)
        this._setBitfieldRange(bitfield.start, bitfield.length, true)
        this.onupdate(0, bitfield, null, null)
        await this._flushOplog()
      }

      this.header.userData = src.header.userData.slice(0)
      const contig = Math.min(src.header.hints.contiguousLength, srcBatch.length)
      if (this.header.hints.contiguousLength < contig) this.header.hints.contiguousLength = contig

      await this._flushOplog()
    } finally {
      src._mutex.unlock()
      this._mutex.unlock()
    }
  }

  async flush () {
    await this._mutex.lock()
    try {
      this._manifestFlushed = true
      this._autoFlush = 4
      await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }
  }

  async _flushOplog () {
    // TODO: the apis using this, actually do not need to wait for the bitfields, tree etc to flush
    // as their mutations are already stored in the oplog. We could potentially just run this in the
    // background. Might be easier to impl that where it is called instead and keep this one simple.
    await this.bitfield.flush()
    await this.tree.flush()

    return flushHeader(this.oplog, this.bigHeader, this.header)
  }

  _appendBlocks (values) {
    return this.blocks.putBatch(this.tree.length, values, this.tree.byteLength)
  }

  async _writeBlock (batch, index, value) {
    const byteOffset = await batch.byteOffset(index * 2)
    await this.blocks.put(index, value, byteOffset)
  }

  async userData (key, value, flush) {
    // TODO: each oplog append can set user data, so we should have a way
    // to just hitch a ride on one of the other ongoing appends?
    await this._mutex.lock()

    try {
      let empty = true

      for (const u of this.header.userData) {
        if (u.key !== key) continue
        if (value && b4a.equals(u.value, value)) return
        empty = false
        break
      }

      if (empty && !value) return

      const entry = {
        userData: { key, value },
        treeNodes: null,
        treeUpgrade: null,
        bitfield: null
      }

      await this.oplog.append([entry], false)

      updateUserData(this.header.userData, key, value)

      if (this._shouldFlush() || flush) await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }
  }

  async truncate (length, fork, { signature, keyPair = this.header.keyPair } = {}) {
    if (this.tree.prologue && length < this.tree.prologue.length) {
      throw INVALID_OPERATION('Truncation breaks prologue')
    }

    this.truncating++
    await this._mutex.lock()

    // upsert compat manifest
    if (this.verifier === null && keyPair) this._setManifest(null, keyPair)

    try {
      const batch = await this.tree.truncate(length, fork)
      if (length > 0) batch.signature = signature || this.verifier.sign(batch, keyPair)
      await this._truncate(batch, null)
    } finally {
      this.truncating--
      this._mutex.unlock()
    }
  }

  async clearBatch () {
    await this._mutex.lock()

    try {
      const len = this.bitfield.findFirst(false, this.tree.length)
      if (len <= this.tree.length) return

      const batch = await this.tree.truncate(this.tree.length, this.tree.fork)

      batch.signature = this.tree.signature // same sig

      const entry = {
        userData: null,
        treeNodes: batch.nodes,
        treeUpgrade: batch,
        bitfield: {
          drop: true,
          start: batch.ancestors,
          length: len - batch.ancestors
        }
      }

      await this.oplog.append([entry], false)

      this._setBitfieldRange(batch.ancestors, len - batch.ancestors, false)
      batch.commit()

      // TODO: (see below todo)
      await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }
  }

  async clear (start, end, cleared) {
    await this._mutex.lock()

    try {
      const entry = {
        userData: null,
        treeNodes: null,
        treeUpgrade: null,
        bitfield: {
          start,
          length: end - start,
          drop: true
        }
      }

      await this.oplog.append([entry], false)

      this._setBitfieldRange(start, end - start, false)

      if (start < this.header.hints.contiguousLength) {
        this.header.hints.contiguousLength = start
      }

      start = this.bitfield.lastSet(start) + 1
      end = this.bitfield.firstSet(end)

      if (end === -1) end = this.tree.length
      if (start >= end || start >= this.tree.length) return

      const offset = await this.tree.byteOffset(start * 2)
      const endOffset = await this.tree.byteOffset(end * 2)
      const length = endOffset - offset

      const before = cleared ? await Info.bytesUsed(this.blocks.storage) : null

      await this.blocks.clear(offset, length)

      const after = cleared ? await Info.bytesUsed(this.blocks.storage) : null

      if (cleared) cleared.blocks = Math.max(before - after, 0)

      this.onupdate(0, entry.bitfield, null, null)

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }
  }

  async purge () {
    return new Promise((resolve, reject) => {
      let missing = 4
      let error = null

      this.oplog.storage.unlink(done)
      this.tree.storage.unlink(done)
      this.bitfield.storage.unlink(done)
      this.blocks.storage.unlink(done)

      function done (err) {
        if (err) error = err
        if (--missing) return
        if (error) reject(error)
        else resolve()
      }
    })
  }

  async insertBatch (batch, values, { signature, keyPair = this.header.keyPair, pending = false, treeLength = batch.treeLength } = {}) {
    await this._mutex.lock()

    try {
      // upsert compat manifest
      if (this.verifier === null && keyPair) this._setManifest(null, keyPair)

      if (this.tree.fork !== batch.fork) return null

      if (this.tree.length > batch.treeLength) {
        if (this.tree.length > batch.length) return null // TODO: partial commit in the future if possible

        for (const root of this.tree.roots) {
          const batchRoot = await batch.get(root.index)
          if (batchRoot.size !== root.size || !b4a.equals(batchRoot.hash, root.hash)) {
            return null
          }
        }
      }

      const adding = batch.length - treeLength

      batch.upgraded = !pending && batch.length > this.tree.length
      batch.treeLength = this.tree.length
      batch.ancestors = this.tree.length
      if (batch.upgraded && !pending) batch.signature = signature || this.verifier.sign(batch, keyPair)

      let byteOffset = batch.byteLength
      for (let i = 0; i < adding; i++) byteOffset -= values[i].byteLength

      if (pending === true) batch.upgraded = false

      const entry = {
        userData: null,
        treeNodes: batch.nodes,
        treeUpgrade: batch.upgraded ? batch : null,
        bitfield: {
          drop: false,
          start: treeLength,
          length: adding
        }
      }

      await this.blocks.putBatch(treeLength, adding < values.length ? values.slice(0, adding) : values, byteOffset)
      await this.oplog.append([entry], false)

      this._setBitfieldRange(entry.bitfield.start, entry.bitfield.length, true)
      batch.commit()

      if (batch.upgraded) {
        this.header.tree.length = batch.length
        this.header.tree.rootHash = batch.hash()
        this.header.tree.signature = batch.signature
      }

      const status = (batch.upgraded ? 0b0001 : 0) | updateContig(this.header, entry.bitfield, this.bitfield)
      if (!pending) {
        // we already commit this, and now we signed it, so tell others
        if (entry.treeUpgrade && treeLength > batch.treeLength) {
          entry.bitfield.start = batch.treeLength
          entry.bitfield.length = treeLength - batch.treeLength
        }

        this.onupdate(status, entry.bitfield, null, null)
      }

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }

    return { length: batch.length, byteLength: batch.byteLength }
  }

  async append (values, { signature, keyPair = this.header.keyPair, preappend } = {}) {
    await this._mutex.lock()

    try {
      // upsert compat manifest
      if (this.verifier === null && keyPair) this._setManifest(null, keyPair)

      if (preappend) await preappend(values)

      if (!values.length) {
        return { length: this.tree.length, byteLength: this.tree.byteLength }
      }

      const batch = this.tree.batch()
      for (const val of values) batch.append(val)

      // only multisig can have prologue so signature is always present
      if (this.tree.prologue && batch.length < this.tree.prologue.length) {
        throw INVALID_OPERATION('Append is not consistent with prologue')
      }

      batch.signature = signature || this.verifier.sign(batch, keyPair)

      const entry = {
        userData: null,
        treeNodes: batch.nodes,
        treeUpgrade: batch,
        bitfield: {
          drop: false,
          start: batch.ancestors,
          length: values.length
        }
      }

      const byteLength = await this._appendBlocks(values)

      await this.oplog.append([entry], false)

      this._setBitfieldRange(batch.ancestors, batch.length - batch.ancestors, true)
      batch.commit()

      this.header.tree.length = batch.length
      this.header.tree.rootHash = batch.hash()
      this.header.tree.signature = batch.signature

      const status = 0b0001 | updateContig(this.header, entry.bitfield, this.bitfield)
      this.onupdate(status, entry.bitfield, null, null)

      if (this._shouldFlush()) await this._flushOplog()

      return { length: batch.length, byteLength }
    } finally {
      this._mutex.unlock()
    }
  }

  _verifyBatchUpgrade (batch, manifest) {
    if (!this.header.manifest) {
      if (!manifest && this.compat) manifest = Verifier.defaultSignerManifest(this.header.key)

      if (!manifest || !(Verifier.isValidManifest(this.header.key, manifest) || (this.compat && Verifier.isCompat(this.header.key, manifest)))) {
        throw INVALID_SIGNATURE('Proof contains an invalid manifest') // TODO: proper error type
      }
    }

    manifest = Verifier.createManifest(manifest) // To unslab

    const verifier = this.verifier || new Verifier(this.header.key, manifest, { crypto: this.crypto, legacy: this._legacy })

    if (!verifier.verify(batch, batch.signature)) {
      throw INVALID_SIGNATURE('Proof contains an invalid signature')
    }

    if (!this.header.manifest) {
      this.header.manifest = manifest
      this.compat = verifier.compat
      this.verifier = verifier
      this.onupdate(0b10000, null, null, null)
    }
  }

  async _verifyExclusive ({ batch, bitfield, value, manifest, from }) {
    this._verifyBatchUpgrade(batch, manifest)

    await this._mutex.lock()

    try {
      if (!batch.commitable()) return false
      this.updating = true

      const entry = {
        userData: null,
        treeNodes: batch.nodes,
        treeUpgrade: batch,
        bitfield
      }

      if (this.preupdate !== null) await this.preupdate(batch, this.header.key)
      if (bitfield) await this._writeBlock(batch, bitfield.start, value)

      await this.oplog.append([entry], false)

      let status = 0b0001

      if (bitfield) {
        this._setBitfield(bitfield.start, true)
        status |= updateContig(this.header, bitfield, this.bitfield)
      }

      batch.commit()

      this.header.tree.fork = batch.fork
      this.header.tree.length = batch.length
      this.header.tree.rootHash = batch.hash()
      this.header.tree.signature = batch.signature

      this.onupdate(status, bitfield, value, from)

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this.updating = false
      this._mutex.unlock()
    }

    return true
  }

  async _verifyShared () {
    if (!this._verifies.length) return false

    await this._mutex.lock()

    const verifies = this._verifies
    this._verifies = null
    this._verified = null

    try {
      const entries = []

      for (const { batch, bitfield, value } of verifies) {
        if (!batch.commitable()) continue

        if (bitfield) {
          await this._writeBlock(batch, bitfield.start, value)
        }

        entries.push({
          userData: null,
          treeNodes: batch.nodes,
          treeUpgrade: null,
          bitfield
        })
      }

      await this.oplog.append(entries, false)

      for (let i = 0; i < verifies.length; i++) {
        const { batch, bitfield, value, manifest, from } = verifies[i]

        if (!batch.commitable()) {
          verifies[i] = null // signal that we cannot commit this one
          continue
        }

        let status = 0

        if (bitfield) {
          this._setBitfield(bitfield.start, true)
          status = updateContig(this.header, bitfield, this.bitfield)
        }

        // if we got a manifest AND its strictly a non compat one, lets store it
        if (manifest && this.header.manifest === null) {
          if (!Verifier.isValidManifest(this.header.key, manifest)) throw INVALID_CHECKSUM('Manifest hash does not match')
          this._setManifest(manifest, null)
        }

        batch.commit()

        this.onupdate(status, bitfield, value, from)
      }

      if (this._shouldFlush()) await this._flushOplog()
    } finally {
      this._mutex.unlock()
    }

    return verifies[0] !== null
  }

  async checkConflict (proof, from) {
    if (this.tree.length < proof.upgrade.length || proof.fork !== this.tree.fork) {
      // out of date this proof - ignore for now
      return false
    }

    const batch = this.tree.verifyFullyRemote(proof)

    try {
      this._verifyBatchUpgrade(batch, proof.manifest)
    } catch {
      return true
    }

    const remoteTreeHash = this.crypto.tree(proof.upgrade.nodes)
    const localTreeHash = this.crypto.tree(await this.tree.getRoots(proof.upgrade.length))

    if (b4a.equals(localTreeHash, remoteTreeHash)) return false

    await this.onconflict(proof)
    return true
  }

  async verifyReorg (proof) {
    const batch = await this.tree.reorg(proof)

    this._verifyBatchUpgrade(batch, proof.manifest)

    return batch
  }

  async verify (proof, from) {
    // We cannot apply "other forks" atm.
    // We should probably still try and they are likely super similar for non upgrades
    // but this is easy atm (and the above layer will just retry)
    if (proof.fork !== this.tree.fork) return false

    const batch = await this.tree.verify(proof)
    if (!batch.commitable()) return false

    const value = (proof.block && proof.block.value) || null
    const op = {
      batch,
      bitfield: value && { drop: false, start: proof.block.index, length: 1 },
      value,
      manifest: proof.manifest,
      from
    }

    if (batch.upgraded) return this._verifyExclusive(op)

    if (this._verifies !== null) {
      const verifies = this._verifies
      const i = verifies.push(op)
      await this._verified
      return verifies[i] !== null
    }

    this._verifies = [op]
    this._verified = this._verifyShared()
    return this._verified
  }

  async reorg (batch, from) {
    if (!batch.commitable()) return false

    this.truncating++
    await this._mutex.lock()

    try {
      if (!batch.commitable()) return false
      await this._truncate(batch, from)
    } finally {
      this.truncating--
      this._mutex.unlock()
    }

    return true
  }

  async _truncate (batch, from) {
    const entry = {
      userData: null,
      treeNodes: batch.nodes,
      treeUpgrade: batch,
      bitfield: {
        drop: true,
        start: batch.ancestors,
        length: this.tree.length - batch.ancestors
      }
    }

    await this.oplog.append([entry], false)

    this._setBitfieldRange(batch.ancestors, this.tree.length - batch.ancestors, false)
    addReorgHint(this.header.hints.reorgs, this.tree, batch)
    batch.commit()

    const contigStatus = updateContig(this.header, entry.bitfield, this.bitfield)
    const status = ((batch.length > batch.ancestors) ? 0b0011 : 0b0010) | contigStatus

    this.header.tree.fork = batch.fork
    this.header.tree.length = batch.length
    this.header.tree.rootHash = batch.hash()
    this.header.tree.signature = batch.signature

    this.onupdate(status, entry.bitfield, null, from)

    // TODO: there is a bug in the merkle tree atm where it cannot handle unflushed
    // truncates if we append or download anything after the truncation point later on
    // This is because tree.get checks the truncated flag. We should fix this so we can do
    // the later flush here as well
    // if (this._shouldFlush()) await this._flushOplog()
    await this._flushOplog()
  }

  openSkipBitfield () {
    if (this.skipBitfield !== null) return this.skipBitfield
    this.skipBitfield = new RemoteBitfield()
    const buf = this.bitfield.toBuffer(this.tree.length)
    const bitfield = new Uint32Array(buf.buffer, buf.byteOffset, buf.byteLength / 4)
    this.skipBitfield.insert(0, bitfield)
    return this.skipBitfield
  }

  _setBitfield (index, value) {
    this.bitfield.set(index, value)
    if (this.skipBitfield !== null) this.skipBitfield.set(index, value)
  }

  _setBitfieldRange (start, length, value) {
    this.bitfield.setRange(start, length, value)
    if (this.skipBitfield !== null) this.skipBitfield.setRange(start, length, value)
  }

  async close () {
    this.closed = true
    await this._mutex.destroy()
    await Promise.allSettled([
      this.oplog.close(),
      this.bitfield.close(),
      this.tree.close(),
      this.blocks.close(),
      this.bigHeader.close()
    ])
  }
}

function updateContig (header, upd, bitfield) {
  const end = upd.start + upd.length

  let c = header.hints.contiguousLength

  if (upd.drop) {
    // If we dropped a block in the current contig range, "downgrade" it
    if (c <= end && c > upd.start) {
      c = upd.start
    }
  } else {
    if (c <= end && c >= upd.start) {
      c = end
      while (bitfield.get(c)) c++
    }
  }

  if (c === header.hints.contiguousLength) {
    return 0b0000
  }

  if (c > header.hints.contiguousLength) {
    header.hints.contiguousLength = c
    return 0b0100
  }

  header.hints.contiguousLength = c
  return 0b1000
}

function addReorgHint (list, tree, batch) {
  if (tree.length === 0 || tree.fork === batch.fork) return

  while (list.length >= 4) list.shift() // 4 here is arbitrary, just want it to be small (hints only)
  while (list.length > 0) {
    if (list[list.length - 1].ancestors > batch.ancestors) list.pop()
    else break
  }

  list.push({ from: tree.fork, to: batch.fork, ancestors: batch.ancestors })
}

function updateUserData (list, key, value) {
  value = unslab(value)

  for (let i = 0; i < list.length; i++) {
    if (list[i].key === key) {
      if (value) list[i].value = value
      else list.splice(i, 1)
      return
    }
  }
  if (value) list.push({ key, value })
}

function closeAll (...storages) {
  let missing = 1
  let error = null

  return new Promise((resolve, reject) => {
    for (const s of storages) {
      missing++
      s.close(done)
    }

    done(null)

    function done (err) {
      if (err) error = err
      if (--missing) return
      if (error) reject(error)
      else resolve()
    }
  })
}

async function flushHeader (oplog, bigHeader, header) {
  if (header.external) {
    await bigHeader.flush(header)
  }

  try {
    await oplog.flush(header)
  } catch (err) {
    if (err.code !== 'OPLOG_HEADER_OVERFLOW') throw err
    await bigHeader.flush(header)
    await oplog.flush(header)
  }
}

function noop () {}

function maximumSegmentStart (start, src, dst) {
  while (true) {
    const a = src.firstSet(start)
    const b = dst.firstUnset(start)

    if (a === -1) return -1
    if (b === -1) return a

    // if dst has the segment, restart
    if (a < b) {
      start = b
      continue
    }

    return a
  }
}

function minimumSegmentEnd (start, src, dst) {
  const a = src.firstUnset(start)
  const b = dst.firstSet(start)

  if (a === -1) return -1
  if (b === -1) return a
  return a < b ? a : b
}
