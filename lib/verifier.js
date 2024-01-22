const defaultCrypto = require('hypercore-crypto')
const b4a = require('b4a')
const c = require('compact-encoding')
const flat = require('flat-tree')
const { BAD_ARGUMENT } = require('hypercore-errors')

const m = require('./messages')
const multisig = require('./multisig')
const caps = require('./caps')

class Signer {
  constructor (crypto, index, { signature = 'ed25519', publicKey, namespace = caps.DEFAULT_NAMESPACE } = {}) {
    if (!publicKey) throw BAD_ARGUMENT('public key is required for a signer')
    if (signature !== 'ed25519') throw BAD_ARGUMENT('Only Ed25519 signatures are supported')

    this.crypto = crypto
    this.signer = index
    this.signature = signature
    this.publicKey = publicKey
    this.namespace = namespace
  }

  verify (batch, signature) {
    return this.crypto.verify(batch.signable(this.namespace), signature, this.publicKey)
  }

  sign (batch, keyPair) {
    return this.crypto.sign(batch.signable(this.namespace), keyPair.secretKey)
  }
}

class CompatSigner extends Signer {
  constructor (crypto, index, signer, legacy) {
    super(crypto, index, signer)
    this.legacy = legacy
  }

  verify (batch, signature) {
    return this.crypto.verify(batch.signableCompat(this.legacy), signature, this.publicKey)
  }

  sign (batch, keyPair) {
    return this.crypto.sign(batch.signableCompat(this.legacy), keyPair.secretKey)
  }
}

module.exports = class Verifier {
  constructor (manifest, { compat = false, crypto = defaultCrypto, legacy = false } = {}) {
    this.compat = compat || manifest === null
    this.version = this.compat ? 0 : typeof manifest.version === 'number' ? manifest.version : 1
    this.hash = manifest.hash || 'blake2b'
    this.allowPatch = !this.compat && !!manifest.allowPatch
    this.quorum = this.compat ? 1 : defaultQuorum(manifest)
    this.signers = manifest.signers
      ? manifest.signers.map((s, index) => this.compat ? new CompatSigner(crypto, index, s, legacy) : new Signer(crypto, index, s))
      : []
    this.prologue = this.compat ? null : (manifest.prologue || null)
  }

  _verifyCompat (batch, signature) {
    if (!signature) return false

    if (this.compat || (!this.allowPatch && this.signers.length === 1)) {
      return !!signature && this.signers[0].verify(batch, signature)
    }

    return this._verifyMulti(batch, signature)
  }

  _inflate (signature) {
    if (this.version >= 1) return multisig.inflate(signature)
    const { proofs, patch } = multisig.inflatev0(signature)

    return {
      proofs: proofs.map(proofToVersion1),
      patch: patch
    }
  }

  _verifyMulti (batch, signature) {
    if (!signature || this.quorum === 0) return false

    const { proofs, patch } = this._inflate(signature)
    if (proofs.length < this.quorum) return false

    const tried = new Uint8Array(this.signers.length)
    const nodes = this.allowPatch && patch.length ? toMap(patch) : null

    for (let i = 0; i < this.quorum; i++) {
      const inp = proofs[i]

      let tree = batch

      if (inp.patch && this.allowPatch) {
        tree = batch.clone()

        const upgrade = generateUpgrade(nodes, batch.length, inp.patch)
        const proof = { fork: tree.fork, block: null, hash: null, seek: null, upgrade, manifest: null }

        try {
          if (!tree.verifyUpgrade(proof)) return false
        } catch {
          return false
        }
      }

      if (inp.signer >= this.signers.length || tried[inp.signer]) return false
      tried[inp.signer] = 1

      const s = this.signers[inp.signer]
      if (!s.verify(tree, inp.signature)) return false
    }

    return true
  }

  verify (batch, signature) {
    if (this.version !== 1) {
      return this._verifyCompat(batch, signature)
    }

    if (this.prologue !== null && batch.length <= this.prologue.length) {
      return batch.length === this.prologue.length && b4a.equals(batch.hash(), this.prologue.hash)
    }

    return this._verifyMulti(batch, signature)
  }

  // TODO: better api for this that is more ... multisig-ey
  sign (batch, keyPair) {
    if (!keyPair || !keyPair.secretKey) throw BAD_ARGUMENT('No key pair was passed')
    if (this.signers.length > 1 || this.allowPatch) throw BAD_ARGUMENT('Can only sign directly for single signers')

    const signature = this.signers[0].sign(batch, keyPair)
    if (this.version !== 1) return signature
    return this.assemble([{ signer: 0, signature, patch: 0, nodes: null }])
  }

  assemble (inputs) {
    return this.version === 0 ? multisig.assemblev0(inputs) : multisig.assemble(inputs)
  }

  manifestHash () {
    return manifestHash(this)
  }

  static manifestHash (manifest) {
    return manifestHash(manifest)
  }

  static defaultSignerManifest (publicKey) {
    return {
      version: 1,
      hash: 'blake2b',
      allowPatch: false,
      quorum: 1,
      signers: [{
        signature: 'ed25519',
        namespace: caps.DEFAULT_NAMESPACE,
        publicKey
      }],
      prologue: null
    }
  }

  static createManifest (inp) {
    if (!inp) return null

    const manifest = {
      version: typeof inp.version === 'number' ? inp.version : 1,
      hash: 'blake2b',
      allowPatch: !!inp.allowPatch,
      quorum: defaultQuorum(inp),
      signers: inp.signers ? inp.signers.map(parseSigner) : [],
      prologue: null
    }

    if (inp.hash && inp.hash !== 'blake2b') throw BAD_ARGUMENT('Only Blake2b hashes are supported')

    if (inp.prologue) {
      if (!(b4a.isBuffer(inp.prologue.hash) && inp.prologue.hash.byteLength === 32) || !(inp.prologue.length >= 0)) {
        throw BAD_ARGUMENT('Invalid prologue')
      }
      manifest.prologue = inp.prologue
    }

    return manifest
  }

  static isValidManifest (key, manifest) {
    return b4a.equals(key, manifestHash(manifest))
  }

  static isCompat (key, manifest) {
    return !!(manifest && manifest.signers.length === 1 && b4a.equals(key, manifest.signers[0].publicKey))
  }

  static sign (manifest, batch, keyPair, opts) {
    const v = new Verifier(manifest, opts)
    return v.sign(batch, keyPair)
  }
}

function toMap (nodes) {
  const m = new Map()
  for (const node of nodes) m.set(node.index, node)
  return m
}

function defaultQuorum (man) {
  if (typeof man.quorum === 'number') return man.quorum
  if (!man.signers || !man.signers.length) return 0
  return (man.signers.length >> 1) + 1
}

function generateUpgrade (patch, start, length) {
  const upgrade = { start, length, nodes: null, additionalNodes: [] }

  const from = start * 2
  const to = from + length * 2

  for (const ite = flat.iterator(0); ite.fullRoot(to); ite.nextTree()) {
    if (ite.index + ite.factor / 2 < from) continue

    if (upgrade.nodes === null && ite.contains(from - 2)) {
      upgrade.nodes = []

      const root = ite.index
      const target = from - 2

      ite.seek(target)

      while (ite.index !== root) {
        ite.sibling()
        if (ite.index > target) upgrade.nodes.push(patch.get(ite.index))
        ite.parent()
      }

      continue
    }

    if (upgrade.nodes === null) upgrade.nodes = []
    upgrade.nodes.push(patch.get(ite.index))
  }

  if (upgrade.nodes === null) upgrade.nodes = []
  return upgrade
}

function parseSigner (signer) {
  validateSigner(signer)
  return {
    signature: 'ed25519',
    namespace: signer.namespace || caps.DEFAULT_NAMESPACE,
    publicKey: signer.publicKey
  }
}

function validateSigner (signer) {
  if (!signer || !signer.publicKey) throw BAD_ARGUMENT('Signer missing public key')
  if (signer.signature && signer.signature !== 'ed25519') throw BAD_ARGUMENT('Only Ed25519 signatures are supported')
}

function manifestHash (manifest) {
  const state = { start: 0, end: 32, buffer: null }
  m.manifest.preencode(state, manifest)
  state.buffer = b4a.allocUnsafe(state.end)
  c.raw.encode(state, caps.MANIFEST)
  m.manifest.encode(state, manifest)
  return defaultCrypto.hash(state.buffer)
}

function proofToVersion1 (proof) {
  return {
    signer: proof.signer,
    signature: proof.signature,
    patch: proof.patch ? proof.patch.length : 0
  }
}
