const defaultCrypto = require('hypercore-crypto')
const b4a = require('b4a')
const c = require('compact-encoding')
const { BAD_ARGUMENT } = require('hypercore-errors')

const m = require('./messages')
const multisig = require('./multisig')
const caps = require('./caps')

module.exports = class Verifier {
  constructor (manifest, { compat = false, crypto = defaultCrypto, legacy = false } = {}) {
    this.crypto = crypto
    this.legacy = legacy
    this.compat = compat || manifest === null
    this.manifest = manifest
    this.version = this.compat ? 0 : typeof this.manifest.version === 'number' ? this.manifest.version : 1
    this.allowPatch = !this.compat && !!this.manifest.allowPatch
    this.quorum = this.compat ? 1 : (this.manifest.quorum || 0)
    this.signers = this.manifest.signers
    this.prologue = this.compat ? null : (this.manifest.prologue || null)
  }

  _verifySignature (tree, signer, signature) {
    return this.crypto.verify(tree.signable(signer.namespace), signature, signer.publicKey)
  }

  _verifyCompat (batch, signature) {
    if (!signature) return false

    if (this.compat) {
      return this.crypto.verify(batch.signableCompat(this.legacy), signature, this.signers[0].publicKey)
    }

    if (!this.allowPatch && this.signers.length === 1) {
      return !!signature && this._verifySignature(batch, this.signers[0], signature)
    }

    return this._verifyMulti(batch, signature)
  }

  _verifyMulti (batch, signature) {
    if (!signature || this.quorum === 0) return false

    const inputs = multisig.inflate(signature)
    if (inputs.length < this.quorum) return false

    const tried = new Uint8Array(this.signers.length)

    for (let i = 0; i < this.quorum; i++) {
      const inp = inputs[i]

      let tree = batch

      if (inp.patch && this.allowPatch) {
        tree = batch.clone()
        const proof = { fork: tree.fork, block: null, hash: null, seek: null, upgrade: inp.patch, manifest: null }

        try {
          if (!tree.verifyUpgrade(proof)) return false
        } catch {
          return false
        }
      }

      if (inp.signer >= this.signers.length || tried[inp.signer]) return false
      tried[inp.signer] = 1

      const s = this.signers[inp.signer]
      if (!this._verifySignature(tree, s, inp.signature)) return false
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

    if (this.compat === true) return this.crypto.sign(batch.signableCompat(this.legacy), keyPair.secretKey)

    const signature = this.crypto.sign(batch.signable(this.signers[0].namespace), keyPair.secretKey)
    if (this.version === 0) return signature

    return multisig.assemble([{ signer: 0, signature, patch: null }])
  }

  manifestHash () {
    return manifestHash(this.manifest)
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
      quorum: inp.quorum || 0,
      signers: inp.signers.map(parseSigner),
      prologue: null
    }

    if (inp.hash && inp.hash !== 'blake2b') throw BAD_ARGUMENT('Only Blake2b hashes are supported')

    if (inp.prologue) {
      if (!(b4a.isBuffer(inp.prologue.hash) && inp.prologue.hash.byteLength === 32) || !inp.prologue.length) {
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
